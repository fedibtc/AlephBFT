use async_trait::async_trait;

use crate::{
    units::UncheckedSignedUnit, Data, Hasher, NodeIndex, Receiver, Sender, Signature, Terminator,
};
use codec::Encode;
use futures::{stream::FusedStream, AsyncWrite, AsyncWriteExt, FutureExt, StreamExt};
use log::{debug, error};

const LOG_TARGET: &str = "AlephBFT-backup-saver";

#[async_trait]
/// Write backups to peristent storage.
pub trait BackupWriter: Send {
    /// Append new data to the backup.
    async fn append(&mut self, data: &[u8]) -> std::io::Result<()>;

    /// Durably append several items, in order, as one write where the
    /// backend allows. The saver hands over every unit already queued, so a
    /// backend that makes each write durable pays that cost once per batch
    /// instead of once per unit.
    async fn append_batch(&mut self, items: &[Vec<u8>]) -> std::io::Result<()> {
        for item in items {
            self.append(item).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<W: AsyncWrite + Send + Unpin> BackupWriter for W {
    async fn append(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.write_all(data).await?;
        self.flush().await
    }

    async fn append_batch(&mut self, items: &[Vec<u8>]) -> std::io::Result<()> {
        for item in items {
            self.write_all(item).await?;
        }
        self.flush().await
    }
}

/// Most units the saver stores in one batch, which bounds how many peer units
/// wait while this node makes no unit of its own.
const MAX_BATCH: usize = 1024;

/// Component responsible for saving units into backup.
/// It waits for items to appear on its receivers, and writes them to backup.
/// It announces a successful write through an appropriate response sender.
pub struct BackupSaver<H: Hasher, D: Data, S: Signature, W: BackupWriter> {
    units_from_runway: Receiver<UncheckedSignedUnit<H, D, S>>,
    responses_for_runway: Sender<UncheckedSignedUnit<H, D, S>>,
    backup: W,
    index: NodeIndex,
}

impl<H: Hasher, D: Data, S: Signature, W: BackupWriter> BackupSaver<H, D, S, W> {
    pub fn new(
        units_from_runway: Receiver<UncheckedSignedUnit<H, D, S>>,
        responses_for_runway: Sender<UncheckedSignedUnit<H, D, S>>,
        backup: W,
        index: NodeIndex,
    ) -> BackupSaver<H, D, S, W> {
        BackupSaver {
            units_from_runway,
            responses_for_runway,
            backup,
            index,
        }
    }

    /// Store `first` and the units after it up to and including this node's
    /// next unit in one batch, then pass them on in order. A unit still
    /// reaches the runway only after it is stored.
    ///
    /// Each batch costs the backend one durable write, typically a disk sync.
    /// This node's unit must be stored before it is broadcast, so it ends the
    /// batch at once; peers' units wait for it, usually about one round. That
    /// turns one sync per unit into about one per round. Units reach the
    /// saver only after they are added to the DAG, so holding them never
    /// delays unit creation. It delays only their re-gossip, which the member
    /// schedules seconds after the save anyway, and their durability, which
    /// recovery does not need: a restarted node fetches missing peer units
    /// again. So no timer ends the hold, even when this node makes no unit
    /// for longer, as in a session's slowdown phase or after its last round;
    /// [`MAX_BATCH`] bounds it.
    ///
    /// An exit signal or the runway closing its channel also ends the batch.
    /// Both stay observable, so the run loop sees them after passing the
    /// batch on: the terminator caches its exit and the channel reports that
    /// it is terminated.
    async fn save_queued(
        &mut self,
        first: UncheckedSignedUnit<H, D, S>,
        terminator: &mut Terminator,
    ) -> Result<Vec<UncheckedSignedUnit<H, D, S>>, std::io::Error> {
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH
            && batch
                .last()
                .is_some_and(|unit| unit.as_signable().creator() != self.index)
        {
            futures::select_biased! {
                _ = terminator.get_exit().fuse() => break,
                unit = self.units_from_runway.next() => match unit {
                    Some(unit) => batch.push(unit),
                    None => break,
                },
            }
        }
        let encoded: Vec<Vec<u8>> = batch.iter().map(|unit| unit.encode()).collect();
        self.backup.append_batch(&encoded).await?;
        Ok(batch)
    }

    pub async fn run(&mut self, mut terminator: Terminator) {
        let mut terminator_exit = false;
        loop {
            // Exit first: a batch that ended on the exit signal must not be
            // followed by another one.
            futures::select_biased! {
                _ = terminator.get_exit().fuse() => {
                    debug!(target: LOG_TARGET, "backup saver received exit signal.");
                    terminator_exit = true;
                },
                unit = self.units_from_runway.next() => {
                    let item = match unit {
                        Some(unit) => unit,
                        None => {
                            error!(target: LOG_TARGET, "receiver of units to save closed early");
                            break;
                        },
                    };
                    let saved = match self.save_queued(item, &mut terminator).await {
                        Ok(saved) => saved,
                        Err(e) => {
                            error!(target: LOG_TARGET, "couldn't save item to backup: {:?}", e);
                            break;
                        }
                    };
                    if saved
                        .into_iter()
                        .any(|unit| self.responses_for_runway.unbounded_send(unit).is_err())
                    {
                        error!(target: LOG_TARGET, "couldn't respond with saved unit to runway");
                        break;
                    }
                    // `select_biased!` skips a terminated stream, so a channel
                    // that closed during the batch is caught here.
                    if self.units_from_runway.is_terminated() {
                        error!(target: LOG_TARGET, "receiver of units to save closed early");
                        break;
                    }
                },
            }

            if terminator_exit {
                debug!(target: LOG_TARGET, "backup saver decided to exit.");
                terminator.terminate_sync().await;
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use codec::Encode;
    use futures::{
        channel::{mpsc, oneshot},
        StreamExt,
    };
    use parking_lot::Mutex;

    use aleph_bft_mock::{Data, Hasher64, Keychain, Saver, Signature};

    use super::MAX_BATCH;
    use crate::{
        backup::{BackupSaver, BackupWriter},
        units::{creator_set, preunit_to_unchecked_signed_unit, UncheckedSignedUnit},
        NodeCount, NodeIndex, Terminator,
    };

    type TestUnit = UncheckedSignedUnit<Hasher64, Data, Signature>;

    /// Units in these tests come from creators 0 to 4.
    const N_MEMBERS: usize = 5;
    /// The saver's own index when the test sends this node's unit last.
    const OWN: NodeIndex = NodeIndex(N_MEMBERS - 1);
    /// An index that created none of the test units.
    const NOT_A_CREATOR: NodeIndex = NodeIndex(N_MEMBERS);
    /// Longer than the saver takes to store and pass on a batch it can end.
    const SETTLE: Duration = Duration::from_millis(200);

    struct PrepareSaverResponse<F: futures::Future> {
        task: F,
        units_for_saver: mpsc::UnboundedSender<TestUnit>,
        units_from_saver: mpsc::UnboundedReceiver<TestUnit>,
        exit_tx: oneshot::Sender<()>,
    }

    fn prepare_saver<W: BackupWriter + Send + 'static>(
        backup: W,
        index: NodeIndex,
    ) -> PrepareSaverResponse<impl futures::Future> {
        let (units_for_saver, units_from_runway) = mpsc::unbounded();
        let (units_for_runway, units_from_saver) = mpsc::unbounded();
        let (exit_tx, exit_rx) = oneshot::channel();

        let task = {
            let mut saver: BackupSaver<Hasher64, Data, Signature, W> =
                BackupSaver::new(units_from_runway, units_for_runway, backup, index);

            async move {
                saver.run(Terminator::create_root(exit_rx, "saver")).await;
            }
        };

        PrepareSaverResponse {
            task,
            units_for_saver,
            units_from_saver,
            exit_tx,
        }
    }

    /// Records each batch it is asked to store, or fails every write.
    #[derive(Clone, Default)]
    struct BatchRecorder {
        batches: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
        fail: bool,
    }

    impl BatchRecorder {
        fn batches(&self) -> Vec<Vec<Vec<u8>>> {
            self.batches.lock().clone()
        }
    }

    #[async_trait]
    impl BackupWriter for BatchRecorder {
        async fn append(&mut self, data: &[u8]) -> std::io::Result<()> {
            self.append_batch(&[data.to_vec()]).await
        }

        async fn append_batch(&mut self, items: &[Vec<u8>]) -> std::io::Result<()> {
            if self.fail {
                return Err(std::io::Error::other("disk full"));
            }
            self.batches.lock().push(items.to_vec());
            Ok(())
        }
    }

    /// `count` distinct units; unit `i` is created by node `i % N_MEMBERS`.
    fn test_units(count: usize) -> Vec<TestUnit> {
        let n_members = NodeCount(N_MEMBERS);
        let creators = creator_set(n_members);
        let keychains: Vec<_> = (0..N_MEMBERS)
            .map(|id| Keychain::new(n_members, NodeIndex(id)))
            .collect();
        (0..count)
            .map(|i| {
                let k = i % N_MEMBERS;
                // The session id keeps units of the same creator distinct.
                preunit_to_unchecked_signed_unit(
                    creators[k].create_unit(0).unwrap().0,
                    (i / N_MEMBERS) as u64,
                    &keychains[k],
                )
            })
            .collect()
    }

    fn encoded(units: &[TestUnit]) -> Vec<Vec<u8>> {
        units.iter().map(|u| u.encode()).collect()
    }

    #[tokio::test]
    async fn test_proper_relative_responses_ordering() {
        let PrepareSaverResponse {
            task,
            units_for_saver,
            mut units_from_saver,
            exit_tx,
        } = prepare_saver(Saver::new(), OWN);

        let handle = tokio::spawn(async {
            task.await;
        });

        let units = test_units(N_MEMBERS);
        for u in units.iter() {
            units_for_saver.unbounded_send(u.clone()).unwrap();
        }

        for u in units {
            let u_backup = units_from_saver.next().await.unwrap();
            assert_eq!(u, u_backup);
        }

        exit_tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn peer_units_wait_for_the_own_unit_and_share_its_batch() {
        let backup = BatchRecorder::default();
        let PrepareSaverResponse {
            task,
            units_for_saver,
            mut units_from_saver,
            exit_tx,
        } = prepare_saver(backup.clone(), OWN);
        let handle = tokio::spawn(async {
            task.await;
        });

        // Units 0 to 3 are peers', unit 4 is this node's.
        let units = test_units(N_MEMBERS);
        for u in &units[..N_MEMBERS - 1] {
            units_for_saver.unbounded_send(u.clone()).unwrap();
        }
        tokio::time::sleep(SETTLE).await;
        assert!(backup.batches().is_empty());
        assert!(
            units_from_saver.try_next().is_err(),
            "a peer unit was passed on"
        );

        units_for_saver
            .unbounded_send(units[N_MEMBERS - 1].clone())
            .unwrap();
        for u in units.iter() {
            assert_eq!(&units_from_saver.next().await.unwrap(), u);
        }
        assert_eq!(backup.batches(), vec![encoded(&units)]);

        exit_tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn the_own_unit_is_stored_at_once() {
        let backup = BatchRecorder::default();
        let PrepareSaverResponse {
            task,
            units_for_saver,
            mut units_from_saver,
            exit_tx,
        } = prepare_saver(backup.clone(), NodeIndex(0));
        let handle = tokio::spawn(async {
            task.await;
        });

        // Unit 0 is this node's; unit 1, a peer's, arrives right behind it.
        let units = test_units(2);
        units_for_saver.unbounded_send(units[0].clone()).unwrap();
        units_for_saver.unbounded_send(units[1].clone()).unwrap();
        let started = std::time::Instant::now();
        assert_eq!(units_from_saver.next().await.unwrap(), units[0]);
        assert!(started.elapsed() < SETTLE);
        assert_eq!(backup.batches()[0], encoded(&units[..1]));

        exit_tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn a_batch_holds_at_most_max_batch_units() {
        let backup = BatchRecorder::default();
        let PrepareSaverResponse {
            task,
            units_for_saver,
            mut units_from_saver,
            exit_tx,
        } = prepare_saver(backup.clone(), NOT_A_CREATOR);

        let units = test_units(MAX_BATCH + 1);
        for u in units.iter() {
            units_for_saver.unbounded_send(u.clone()).unwrap();
        }
        let handle = tokio::spawn(async {
            task.await;
        });

        for u in &units[..MAX_BATCH] {
            assert_eq!(&units_from_saver.next().await.unwrap(), u);
        }
        // The last unit waits for this node's unit, which never comes, with
        // no timer to end the wait; the exit signal stores it.
        tokio::time::sleep(SETTLE).await;
        assert!(units_from_saver.try_next().is_err());
        assert_eq!(backup.batches(), vec![encoded(&units[..MAX_BATCH])]);

        exit_tx.send(()).unwrap();
        handle.await.unwrap();
        assert_eq!(
            units_from_saver.next().await.as_ref(),
            units.last(),
            "the held unit is passed on at exit"
        );
        let sizes: Vec<usize> = backup.batches().iter().map(Vec::len).collect();
        assert_eq!(sizes, vec![MAX_BATCH, 1]);
        assert_eq!(backup.batches().concat(), encoded(&units));
    }

    #[tokio::test]
    async fn units_are_not_passed_on_when_the_batch_fails() {
        let backup = BatchRecorder {
            fail: true,
            ..Default::default()
        };
        let PrepareSaverResponse {
            task,
            units_for_saver,
            mut units_from_saver,
            exit_tx: _exit_tx,
        } = prepare_saver(backup, OWN);

        for u in test_units(N_MEMBERS) {
            units_for_saver.unbounded_send(u).unwrap();
        }
        // The saver stops on the failed write and drops its sender.
        task.await;
        assert_eq!(units_from_saver.next().await, None);
    }

    #[tokio::test]
    async fn an_exit_signal_ends_the_hold_and_stores_what_arrived() {
        let backup = BatchRecorder::default();
        let PrepareSaverResponse {
            task,
            units_for_saver,
            mut units_from_saver,
            exit_tx,
        } = prepare_saver(backup.clone(), NOT_A_CREATOR);
        let handle = tokio::spawn(async {
            task.await;
        });

        let units = test_units(2);
        for u in units.iter() {
            units_for_saver.unbounded_send(u.clone()).unwrap();
        }
        tokio::time::sleep(SETTLE).await;
        let started = std::time::Instant::now();
        exit_tx.send(()).unwrap();
        handle.await.unwrap();
        assert!(started.elapsed() < SETTLE);
        assert_eq!(backup.batches(), vec![encoded(&units)]);
        for u in units.iter() {
            assert_eq!(&units_from_saver.next().await.unwrap(), u);
        }
    }

    #[tokio::test]
    async fn the_hold_ends_early_when_the_runway_closes() {
        let backup = BatchRecorder::default();
        let PrepareSaverResponse {
            task,
            units_for_saver,
            mut units_from_saver,
            exit_tx: _exit_tx,
        } = prepare_saver(backup.clone(), NOT_A_CREATOR);

        let units = test_units(2);
        for u in units.iter() {
            units_for_saver.unbounded_send(u.clone()).unwrap();
        }
        drop(units_for_saver);
        let started = std::time::Instant::now();
        task.await;
        assert!(started.elapsed() < SETTLE);
        // What arrived is still stored and passed on.
        assert_eq!(backup.batches(), vec![encoded(&units)]);
        for u in units.iter() {
            assert_eq!(&units_from_saver.next().await.unwrap(), u);
        }
    }
}
