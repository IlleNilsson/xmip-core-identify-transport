#![forbid(unsafe_code)]

//! Identify by transport: one named property the carrier promoted is the
//! claim.
//!
//! Many protocols make the peer name itself before it sends anything, and the
//! name is neither a header nor a credential: an MQTT client id in CONNECT, an
//! AMQP container id in `open`, a Kafka client id on every request, the user
//! an SSH session was opened as, the client end of a named pipe. The carrier
//! that speaks the protocol promotes that name onto the arrival in its own
//! vocabulary, and this identifier is built naming one such property. What it
//! finds there it presents under [`xcore::mechanism::transport`], passed and
//! with nothing behind it: the peer said so, and no more is known.
//!
//! A carrier with no vocabulary of its own for the name promotes it as
//! `transport.peer`, and that is what [`TransportProperty::default`] reads:
//!
//! ```text
//! transport.peer      the peer's own name for itself, whatever the protocol calls it
//! mqtt.client-id      an example of a carrier's own word, read with `named`
//! ```
//!
//! Only a pushed Stream has a peer that named itself. Where Xmip went and
//! fetched the Stream, or found it waiting, the name on the connection was
//! Xmip's own and says nothing about the source.
//!
//! Evidence this technology writes: `transport.property`, the name of the
//! property the claim was read from. It attaches no proof.

use identify::{IdentifyError, Presented, StreamArrival, TransportIdentifier};
use xcore::{Arriving, Mechanism};

/// The property a carrier promotes the peer's name under where its protocol
/// has no better word.
pub const PEER: &str = "transport.peer";
/// The evidence name the property's name rides under.
pub const PROPERTY: &str = "transport.property";

/// Reads one named property the carrier promoted.
#[derive(Clone, Debug)]
pub struct TransportProperty {
    property: String,
}

impl TransportProperty {
    /// Read this property, in the carrier's own vocabulary: `mqtt.client-id`,
    /// `amqp.container-id`, `ssh.user`.
    ///
    /// # Errors
    ///
    /// Where the name is empty: an identifier that could never match is a
    /// configuration fault, said when the identifier is built.
    pub fn named(property: &str) -> Result<Self, IdentifyError> {
        let property = property.trim();
        if property.is_empty() {
            return Err(IdentifyError::new(
                "the transport property to read has no name",
            ));
        }

        Ok(Self {
            property: property.to_string(),
        })
    }

    /// The property this reads.
    #[must_use]
    pub fn property(&self) -> &str {
        &self.property
    }
}

impl Default for TransportProperty {
    /// Reads [`PEER`].
    fn default() -> Self {
        Self {
            property: PEER.to_string(),
        }
    }
}

impl TransportIdentifier for TransportProperty {
    fn mechanism(&self) -> Mechanism {
        xcore::mechanism::transport()
    }

    fn identify(&self, arrival: &StreamArrival<'_>) -> Result<Option<Presented>, IdentifyError> {
        if arrival.arriving() != Arriving::Pushed {
            return Ok(None);
        }

        let Some(value) = arrival.property(&self.property) else {
            return Ok(None);
        };

        let value = value.trim();
        if value.is_empty() {
            return Err(IdentifyError::new(format!(
                "the carrier promoted {} and left it empty",
                self.property
            )));
        }

        Ok(Some(
            Presented::passed(self.mechanism(), value).with_evidence(PROPERTY, &self.property),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stream::Stream;
    use xcore::{Established, Layer, StreamId};

    fn stream() -> Stream {
        Stream::new(StreamId::new(1), b"<order/>".to_vec(), None)
    }

    fn facts(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn a_promoted_client_id_is_the_claim_and_the_property_is_on_the_record() {
        let stream = stream();
        let facts = facts(&[
            ("mqtt.client-id", "press-line-4"),
            ("peer.address", "10.0.0.7:51022"),
        ]);
        let arrival = StreamArrival::new(&stream, Arriving::Pushed, "mqtt://xmip/press", &facts);

        let claim = TransportProperty::named("mqtt.client-id")
            .expect("a name")
            .identify(&arrival)
            .expect("read")
            .expect("a claim");

        assert_eq!(claim.mechanism.name(), "transport");
        assert_eq!(claim.value, "press-line-4");
        assert_eq!(claim.established, Established::Passed);
        assert_eq!(claim.layer(), Layer::Transport);
        assert_eq!(
            claim.evidence,
            vec![(PROPERTY.to_string(), "mqtt.client-id".to_string())]
        );
    }

    #[test]
    fn the_default_reads_the_shared_name_for_a_peer() {
        let stream = stream();
        let facts = facts(&[(PEER, "container-7")]);
        let arrival = StreamArrival::new(&stream, Arriving::Pushed, "amqp://xmip/in", &facts);

        let claim = TransportProperty::default()
            .identify(&arrival)
            .expect("read")
            .expect("a claim");

        assert_eq!(claim.value, "container-7");
    }

    #[test]
    fn an_arrival_the_carrier_promoted_nothing_onto_presents_nothing() {
        let stream = stream();
        let facts = facts(&[("kafka.client-id", "billing")]);
        let arrival = StreamArrival::new(&stream, Arriving::Pushed, "kafka://xmip/in", &facts);

        assert!(
            TransportProperty::default()
                .identify(&arrival)
                .expect("read")
                .is_none()
        );
    }

    #[test]
    fn a_property_promoted_and_left_empty_is_an_error_and_not_an_absence() {
        let stream = stream();
        let facts = facts(&[(PEER, "  ")]);
        let arrival = StreamArrival::new(&stream, Arriving::Pushed, "amqp://xmip/in", &facts);

        let failure = TransportProperty::default()
            .identify(&arrival)
            .expect_err("empty");

        assert_eq!(
            failure.to_string(),
            "the carrier promoted transport.peer and left it empty"
        );
    }

    #[test]
    fn an_identifier_naming_no_property_is_refused_when_it_is_built() {
        let failure = TransportProperty::named(" ").expect_err("no name");

        assert_eq!(
            failure.to_string(),
            "the transport property to read has no name"
        );
    }

    #[test]
    fn a_detected_or_scheduled_arrival_has_no_peer_that_named_itself() {
        let stream = stream();
        let facts = facts(&[(PEER, "xmip-node-1")]);

        for arriving in [Arriving::Detected, Arriving::Scheduled] {
            let arrival = StreamArrival::new(&stream, arriving, "kafka://broker/topic", &facts);

            assert!(
                TransportProperty::default()
                    .identify(&arrival)
                    .expect("read")
                    .is_none()
            );
        }
    }
}
