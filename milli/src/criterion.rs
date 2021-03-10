use std::collections::HashMap;
use std::fmt;

use anyhow::{Context, bail};
use regex::Regex;
use serde::{Serialize, Deserialize};

use crate::facet::FacetType;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Criterion {
    /// Sorted by increasing number of typos.
    Typo,
    /// Sorted by decreasing number of matched query terms.
    Words,
    /// Sorted by increasing distance between matched query terms.
    Proximity,
    /// Documents with quey words contained in more important
    /// attributes are considred better.
    Attribute,
    /// Sorted by the similarity of the matched words with the query words.
    Exactness,
    /// Sorted by the increasing value of the field specified.
    Asc(String),
    /// Sorted by the decreasing value of the field specified.
    Desc(String),
}

impl Criterion {
    pub fn from_str(faceted_attributes: &HashMap<String, FacetType>, txt: &str) -> anyhow::Result<Criterion> {
        match txt {
            "typo" => Ok(Criterion::Typo),
            "words" => Ok(Criterion::Words),
            "proximity" => Ok(Criterion::Proximity),
            "attribute" => Ok(Criterion::Attribute),
            "exactness" => Ok(Criterion::Exactness),
            text => {
                let re = Regex::new(r#"(asc|desc)\(([\w_-]+)\)"#)?;
                let caps = re.captures(text).with_context(|| format!("unknown criterion name: {}", text))?;
                let order = caps.get(1).unwrap().as_str();
                let field_name = caps.get(2).unwrap().as_str();
                faceted_attributes.get(field_name).with_context(|| format!("Can't use {:?} as a criterion as it isn't a faceted field.", field_name))?;
                match order {
                    "asc" => Ok(Criterion::Asc(field_name.to_string())),
                    "desc" => Ok(Criterion::Desc(field_name.to_string())),
                    otherwise => bail!("unknown criterion name: {}", otherwise),
                }
            },
        }
    }
}

pub fn default_criteria() -> Vec<Criterion> {
    vec![
        Criterion::Words,
        Criterion::Typo,
        Criterion::Proximity,
        Criterion::Attribute,
        Criterion::Exactness,
    ]
}

impl fmt::Display for Criterion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Criterion::*;

        match self {
            Typo            => f.write_str("typo"),
            Words           => f.write_str("words"),
            Proximity       => f.write_str("proximity"),
            Attribute       => f.write_str("attribute"),
            Exactness       => f.write_str("exactness"),
            Asc(attr)       => write!(f, "asc({})", attr),
            Desc(attr)      => write!(f, "desc({})", attr),
        }
    }
}
