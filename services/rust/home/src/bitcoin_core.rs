//! New blocks, straight from Bitcoin Core.
//!
//! bitcoind can announce every new block over ZeroMQ the moment it accepts
//! it (`zmqpubrawblock` in bitcoin.conf). It is a radio broadcast: whoever
//! is tuned in hears it, and nothing is replayed for someone who was not.
//!
//! On the Pi that broadcast is only on 127.0.0.1, so we reach it through an
//! SSH tunnel: a local port here that leads to that port there.

use std::process::Command;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use bitcoin::{Address, Network, ScriptBuf};
use tokio::sync::mpsc;

use crate::ckpool::{pi, ssh_options};
use crate::setting;

/// Where the tunnel comes out on this side.
const LOCAL_END: &str = "127.0.0.1:28332";

/// A block as bitcoind announced it.
pub struct NewBlock {
    pub height: u64,
    block: bitcoin::Block,
}

impl NewBlock {
    pub fn from_bytes(raw: &[u8]) -> Result<NewBlock> {
        let block: bitcoin::Block = bitcoin::consensus::deserialize(raw).context("not a block")?;
        // Every block writes its own height into its first transaction.
        let height = block.bip34_block_height().context("block without a height")?;
        Ok(NewBlock { height, block })
    }

    /// Does this block's reward go to that address? The first transaction
    /// of a block is the miner paying itself, so that is where to look.
    pub fn pays(&self, address: &PayoutAddress) -> bool {
        let Some(reward) = self.block.coinbase() else { return false };
        reward.output.iter().any(|output| output.script_pubkey == address.0)
    }
}

/// A Bitcoin address, in the form it takes inside a transaction.
pub struct PayoutAddress(ScriptBuf);

impl PayoutAddress {
    pub fn parse(address: &str) -> Result<PayoutAddress> {
        let address = Address::from_str(address.trim()).with_context(|| format!("'{address}' is not a Bitcoin address"))?;
        Ok(PayoutAddress(address.require_network(Network::Bitcoin)?.script_pubkey()))
    }
}

/// Every block bitcoind announces from now on, as it happens.
pub fn new_blocks() -> mpsc::UnboundedReceiver<NewBlock> {
    let (heard, new_blocks) = mpsc::unbounded_channel();
    thread::spawn(keep_the_tunnel_open);
    thread::spawn(move || {
        loop {
            if let Err(problem) = listen(&heard) {
                println!("lost Bitcoin Core's block announcements ({problem}), listening again");
                thread::sleep(Duration::from_secs(5));
            }
        }
    });
    new_blocks
}

/// Runs `ssh -L`, and runs it again whenever it ends.
fn keep_the_tunnel_open() {
    let far_end = setting("BITCOIN_ZMQ_ON_PI", "127.0.0.1:28332");
    loop {
        let ended = Command::new("ssh")
            .args(ssh_options())
            // Notice a dead connection within about a minute, and don't
            // pretend to be up if the tunnel itself could not be made.
            .args(["-o", "ServerAliveInterval=20", "-o", "ServerAliveCountMax=3", "-o", "ExitOnForwardFailure=yes"])
            .args(["-N", "-L", &format!("{LOCAL_END}:{far_end}")])
            .arg(pi())
            .status();
        println!("the tunnel to the Pi closed ({ended:?}), opening it again");
        thread::sleep(Duration::from_secs(5));
    }
}

fn listen(heard: &mpsc::UnboundedSender<NewBlock>) -> Result<()> {
    let socket = zmq::Context::new().socket(zmq::SUB)?;
    socket.connect(&format!("tcp://{LOCAL_END}"))?;
    socket.set_subscribe(b"rawblock")?;
    println!("listening for new blocks from Bitcoin Core");

    let mut expected = None;
    loop {
        // Each announcement has three parts: the topic, the block itself,
        // and a counter that goes up by one every time.
        let parts = socket.recv_multipart(0)?;
        let [_topic, raw_block, counter] = parts.as_slice() else { continue };

        let counter = u32::from_le_bytes(counter.as_slice().try_into().unwrap_or_default());
        if expected.is_some_and(|expected| expected != counter) {
            // Not fatal: the ten-minute look at ckpool's log catches a
            // found block that was announced while we were not listening.
            println!("missed some block announcements");
        }
        expected = Some(counter.wrapping_add(1));

        match NewBlock::from_bytes(raw_block) {
            Ok(block) => heard.send(block)?,
            Err(problem) => println!("could not read a block announcement: {problem}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real block with nothing in it but the miner's reward, which keeps
    /// it small enough to store here. Block 975040, mined by AntPool.
    fn block_975040() -> NewBlock {
        let hex = include_str!("block_975040.hex").trim();
        let raw: Vec<u8> = (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect();
        NewBlock::from_bytes(&raw).unwrap()
    }

    #[test]
    fn a_block_knows_its_height() {
        assert_eq!(block_975040().height, 975040);
    }

    #[test]
    fn a_block_pays_whoever_mined_it_and_nobody_else() {
        let antpool = PayoutAddress::parse("39C7fxSzEACPjM78Z7xdPxhf7mKxJwvfMJ").unwrap();
        let somebody_else = PayoutAddress::parse("bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq").unwrap();
        assert!(block_975040().pays(&antpool));
        assert!(!block_975040().pays(&somebody_else));
    }

    #[test]
    fn nonsense_is_not_an_address_or_a_block() {
        assert!(PayoutAddress::parse("not-an-address").is_err());
        assert!(NewBlock::from_bytes(b"not a block").is_err());
    }
}
