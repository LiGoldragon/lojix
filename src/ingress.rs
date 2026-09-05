#![allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InspectionRequest {
    InspectStore(InspectStore),
}
impl datom_codec::Datomic for InspectionRequest {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let v = datom_codec::Sited::variant(site)?;
        match v.name {
            "InspectStore" => {
                std::result::Result::Ok(Self::InspectStore(datom_codec::Carrying::body(v)?))
            }
            _ => std::result::Result::Err(datom_codec::Headed::reject(
                &v,
                datom_codec::Problem::UnknownVariant(
                    protos::Word::try_from(v.name).expect("variant name"),
                ),
            )),
        }
    }
}
impl protos::Conceivable<datom_codec::Datom> for InspectionRequest {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            match self {
                Self::InspectStore(p0) => datom_codec::Datom::Variant(
                    protos::Symbol::try_from("InspectStore").expect("static variant"),
                    std::boxed::Box::new(
                        protos::Conceivable::conceive(p0)
                            .expect("infallible datom ascent")
                            .1,
                    ),
                ),
            },
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectStore(pub protos::Text);
impl datom_codec::Datomic for InspectStore {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let mut p = datom_codec::Sited::positions(site, 1)?;
        let p0: protos::Text = datom_codec::Positional::position(&mut p)?;
        std::result::Result::Ok(Self(p0))
    }
}
impl protos::Conceivable<datom_codec::Datom> for InspectStore {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            datom_codec::Datom::Struct(vec![
                protos::Conceivable::conceive(&self.0)
                    .expect("infallible datom ascent")
                    .1,
            ]),
        ))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResetStore {
    ResetStore,
}
impl datom_codec::Datomic for ResetStore {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let v = datom_codec::Sited::variant(site)?;
        match v.name {
            "ResetStore" => {
                datom_codec::Headed::nothing(v)?;
                std::result::Result::Ok(Self::ResetStore)
            }
            _ => std::result::Result::Err(datom_codec::Headed::reject(
                &v,
                datom_codec::Problem::UnknownVariant(
                    protos::Word::try_from(v.name).expect("variant name"),
                ),
            )),
        }
    }
}
impl protos::Conceivable<datom_codec::Datom> for ResetStore {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            match self {
                Self::ResetStore => datom_codec::Datom::Word(
                    datom_codec::DatomWord::try_from(
                        protos::Word::try_from("ResetStore").expect("static variant"),
                    )
                    .expect("stable variant"),
                ),
            },
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapRequest {
    BootstrapRun(BootstrapRun),
}
impl datom_codec::Datomic for BootstrapRequest {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let v = datom_codec::Sited::variant(site)?;
        match v.name {
            "BootstrapRun" => {
                std::result::Result::Ok(Self::BootstrapRun(datom_codec::Carrying::body(v)?))
            }
            _ => std::result::Result::Err(datom_codec::Headed::reject(
                &v,
                datom_codec::Problem::UnknownVariant(
                    protos::Word::try_from(v.name).expect("variant name"),
                ),
            )),
        }
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapRequest {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            match self {
                Self::BootstrapRun(p0) => datom_codec::Datom::Variant(
                    protos::Symbol::try_from("BootstrapRun").expect("static variant"),
                    std::boxed::Box::new(
                        protos::Conceivable::conceive(p0)
                            .expect("infallible datom ascent")
                            .1,
                    ),
                ),
            },
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapRun(pub BootstrapRequestId, pub BootstrapMode);
impl datom_codec::Datomic for BootstrapRun {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let mut p = datom_codec::Sited::positions(site, 2)?;
        let p0: BootstrapRequestId = datom_codec::Positional::position(&mut p)?;
        let p1: BootstrapMode = datom_codec::Positional::position(&mut p)?;
        std::result::Result::Ok(Self(p0, p1))
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapRun {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            datom_codec::Datom::Struct(vec![
                protos::Conceivable::conceive(&self.0)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.1)
                    .expect("infallible datom ascent")
                    .1,
            ]),
        ))
    }
}
pub type BootstrapRequestId = protos::Text;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapMode {
    BuildOnly(BootstrapBuildOnly),
    BootOnce(BootstrapBootOnce),
}
impl datom_codec::Datomic for BootstrapMode {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let v = datom_codec::Sited::variant(site)?;
        match v.name {
            "BuildOnly" => {
                std::result::Result::Ok(Self::BuildOnly(datom_codec::Carrying::body(v)?))
            }
            "BootOnce" => std::result::Result::Ok(Self::BootOnce(datom_codec::Carrying::body(v)?)),
            _ => std::result::Result::Err(datom_codec::Headed::reject(
                &v,
                datom_codec::Problem::UnknownVariant(
                    protos::Word::try_from(v.name).expect("variant name"),
                ),
            )),
        }
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapMode {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            match self {
                Self::BuildOnly(p0) => datom_codec::Datom::Variant(
                    protos::Symbol::try_from("BuildOnly").expect("static variant"),
                    std::boxed::Box::new(
                        protos::Conceivable::conceive(p0)
                            .expect("infallible datom ascent")
                            .1,
                    ),
                ),
                Self::BootOnce(p0) => datom_codec::Datom::Variant(
                    protos::Symbol::try_from("BootOnce").expect("static variant"),
                    std::boxed::Box::new(
                        protos::Conceivable::conceive(p0)
                            .expect("infallible datom ascent")
                            .1,
                    ),
                ),
            },
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapBuildOnly(
    pub BootstrapInput,
    pub BootstrapBuilder,
    pub BootstrapJournalParent,
    pub BootstrapGcRootPath,
    pub BootstrapTerminalEvidencePath,
);
impl datom_codec::Datomic for BootstrapBuildOnly {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let mut p = datom_codec::Sited::positions(site, 5)?;
        let p0: BootstrapInput = datom_codec::Positional::position(&mut p)?;
        let p1: BootstrapBuilder = datom_codec::Positional::position(&mut p)?;
        let p2: BootstrapJournalParent = datom_codec::Positional::position(&mut p)?;
        let p3: BootstrapGcRootPath = datom_codec::Positional::position(&mut p)?;
        let p4: BootstrapTerminalEvidencePath = datom_codec::Positional::position(&mut p)?;
        std::result::Result::Ok(Self(p0, p1, p2, p3, p4))
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapBuildOnly {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            datom_codec::Datom::Struct(vec![
                protos::Conceivable::conceive(&self.0)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.1)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.2)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.3)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.4)
                    .expect("infallible datom ascent")
                    .1,
            ]),
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapBootOnce(
    pub BootstrapInput,
    pub BootstrapBuilder,
    pub BootstrapTestPlan,
    pub BootstrapActivationBackend,
    pub BootstrapJournalParent,
    pub BootstrapGcRootPath,
    pub BootstrapTerminalEvidencePath,
);
impl datom_codec::Datomic for BootstrapBootOnce {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let mut p = datom_codec::Sited::positions(site, 7)?;
        let p0: BootstrapInput = datom_codec::Positional::position(&mut p)?;
        let p1: BootstrapBuilder = datom_codec::Positional::position(&mut p)?;
        let p2: BootstrapTestPlan = datom_codec::Positional::position(&mut p)?;
        let p3: BootstrapActivationBackend = datom_codec::Positional::position(&mut p)?;
        let p4: BootstrapJournalParent = datom_codec::Positional::position(&mut p)?;
        let p5: BootstrapGcRootPath = datom_codec::Positional::position(&mut p)?;
        let p6: BootstrapTerminalEvidencePath = datom_codec::Positional::position(&mut p)?;
        std::result::Result::Ok(Self(p0, p1, p2, p3, p4, p5, p6))
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapBootOnce {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            datom_codec::Datom::Struct(vec![
                protos::Conceivable::conceive(&self.0)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.1)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.2)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.3)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.4)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.5)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.6)
                    .expect("infallible datom ascent")
                    .1,
            ]),
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapInput {
    Direct(BootstrapDirectInput),
    Horizon(BootstrapHorizonInput),
}
impl datom_codec::Datomic for BootstrapInput {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let v = datom_codec::Sited::variant(site)?;
        match v.name {
            "Direct" => std::result::Result::Ok(Self::Direct(datom_codec::Carrying::body(v)?)),
            "Horizon" => std::result::Result::Ok(Self::Horizon(datom_codec::Carrying::body(v)?)),
            _ => std::result::Result::Err(datom_codec::Headed::reject(
                &v,
                datom_codec::Problem::UnknownVariant(
                    protos::Word::try_from(v.name).expect("variant name"),
                ),
            )),
        }
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapInput {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            match self {
                Self::Direct(p0) => datom_codec::Datom::Variant(
                    protos::Symbol::try_from("Direct").expect("static variant"),
                    std::boxed::Box::new(
                        protos::Conceivable::conceive(p0)
                            .expect("infallible datom ascent")
                            .1,
                    ),
                ),
                Self::Horizon(p0) => datom_codec::Datom::Variant(
                    protos::Symbol::try_from("Horizon").expect("static variant"),
                    std::boxed::Box::new(
                        protos::Conceivable::conceive(p0)
                            .expect("infallible datom ascent")
                            .1,
                    ),
                ),
            },
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapDirectInput(
    pub BootstrapFlakeReference,
    pub BootstrapNixSystem,
    pub BootstrapOutputSelector,
);
impl datom_codec::Datomic for BootstrapDirectInput {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let mut p = datom_codec::Sited::positions(site, 3)?;
        let p0: BootstrapFlakeReference = datom_codec::Positional::position(&mut p)?;
        let p1: BootstrapNixSystem = datom_codec::Positional::position(&mut p)?;
        let p2: BootstrapOutputSelector = datom_codec::Positional::position(&mut p)?;
        std::result::Result::Ok(Self(p0, p1, p2))
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapDirectInput {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            datom_codec::Datom::Struct(vec![
                protos::Conceivable::conceive(&self.0)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.1)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.2)
                    .expect("infallible datom ascent")
                    .1,
            ]),
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapHorizonInput(
    pub BootstrapProposalSource,
    pub BootstrapClusterName,
    pub BootstrapNodeName,
    pub BootstrapMaterializationShape,
    pub BootstrapSecretsInput,
    pub BootstrapFlakeReference,
    pub BootstrapNixSystem,
    pub BootstrapOutputSelector,
);
impl datom_codec::Datomic for BootstrapHorizonInput {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let mut p = datom_codec::Sited::positions(site, 8)?;
        let p0: BootstrapProposalSource = datom_codec::Positional::position(&mut p)?;
        let p1: BootstrapClusterName = datom_codec::Positional::position(&mut p)?;
        let p2: BootstrapNodeName = datom_codec::Positional::position(&mut p)?;
        let p3: BootstrapMaterializationShape = datom_codec::Positional::position(&mut p)?;
        let p4: BootstrapSecretsInput = datom_codec::Positional::position(&mut p)?;
        let p5: BootstrapFlakeReference = datom_codec::Positional::position(&mut p)?;
        let p6: BootstrapNixSystem = datom_codec::Positional::position(&mut p)?;
        let p7: BootstrapOutputSelector = datom_codec::Positional::position(&mut p)?;
        std::result::Result::Ok(Self(p0, p1, p2, p3, p4, p5, p6, p7))
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapHorizonInput {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            datom_codec::Datom::Struct(vec![
                protos::Conceivable::conceive(&self.0)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.1)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.2)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.3)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.4)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.5)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.6)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.7)
                    .expect("infallible datom ascent")
                    .1,
            ]),
        ))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapMaterializationShape {
    CompleteHost,
    BaseHost,
}
impl datom_codec::Datomic for BootstrapMaterializationShape {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let v = datom_codec::Sited::variant(site)?;
        match v.name {
            "CompleteHost" => {
                datom_codec::Headed::nothing(v)?;
                std::result::Result::Ok(Self::CompleteHost)
            }
            "BaseHost" => {
                datom_codec::Headed::nothing(v)?;
                std::result::Result::Ok(Self::BaseHost)
            }
            _ => std::result::Result::Err(datom_codec::Headed::reject(
                &v,
                datom_codec::Problem::UnknownVariant(
                    protos::Word::try_from(v.name).expect("variant name"),
                ),
            )),
        }
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapMaterializationShape {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            match self {
                Self::CompleteHost => datom_codec::Datom::Word(
                    datom_codec::DatomWord::try_from(
                        protos::Word::try_from("CompleteHost").expect("static variant"),
                    )
                    .expect("stable variant"),
                ),
                Self::BaseHost => datom_codec::Datom::Word(
                    datom_codec::DatomWord::try_from(
                        protos::Word::try_from("BaseHost").expect("static variant"),
                    )
                    .expect("stable variant"),
                ),
            },
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapSecretsInput {
    NoSecrets,
    SecretsDirectory(BootstrapSecretsDirectory),
}
impl datom_codec::Datomic for BootstrapSecretsInput {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let v = datom_codec::Sited::variant(site)?;
        match v.name {
            "NoSecrets" => {
                datom_codec::Headed::nothing(v)?;
                std::result::Result::Ok(Self::NoSecrets)
            }
            "SecretsDirectory" => {
                std::result::Result::Ok(Self::SecretsDirectory(datom_codec::Carrying::body(v)?))
            }
            _ => std::result::Result::Err(datom_codec::Headed::reject(
                &v,
                datom_codec::Problem::UnknownVariant(
                    protos::Word::try_from(v.name).expect("variant name"),
                ),
            )),
        }
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapSecretsInput {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            match self {
                Self::NoSecrets => datom_codec::Datom::Word(
                    datom_codec::DatomWord::try_from(
                        protos::Word::try_from("NoSecrets").expect("static variant"),
                    )
                    .expect("stable variant"),
                ),
                Self::SecretsDirectory(p0) => datom_codec::Datom::Variant(
                    protos::Symbol::try_from("SecretsDirectory").expect("static variant"),
                    std::boxed::Box::new(
                        protos::Conceivable::conceive(p0)
                            .expect("infallible datom ascent")
                            .1,
                    ),
                ),
            },
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapBuilder {
    NoBuilder,
    NixBuilder(BootstrapBuilderSpec),
}
impl datom_codec::Datomic for BootstrapBuilder {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let v = datom_codec::Sited::variant(site)?;
        match v.name {
            "NoBuilder" => {
                datom_codec::Headed::nothing(v)?;
                std::result::Result::Ok(Self::NoBuilder)
            }
            "NixBuilder" => {
                std::result::Result::Ok(Self::NixBuilder(datom_codec::Carrying::body(v)?))
            }
            _ => std::result::Result::Err(datom_codec::Headed::reject(
                &v,
                datom_codec::Problem::UnknownVariant(
                    protos::Word::try_from(v.name).expect("variant name"),
                ),
            )),
        }
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapBuilder {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            match self {
                Self::NoBuilder => datom_codec::Datom::Word(
                    datom_codec::DatomWord::try_from(
                        protos::Word::try_from("NoBuilder").expect("static variant"),
                    )
                    .expect("stable variant"),
                ),
                Self::NixBuilder(p0) => datom_codec::Datom::Variant(
                    protos::Symbol::try_from("NixBuilder").expect("static variant"),
                    std::boxed::Box::new(
                        protos::Conceivable::conceive(p0)
                            .expect("infallible datom ascent")
                            .1,
                    ),
                ),
            },
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapTestPlan {
    NoTest,
    RunHermeticTest(BootstrapHermeticTest),
}
impl datom_codec::Datomic for BootstrapTestPlan {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let v = datom_codec::Sited::variant(site)?;
        match v.name {
            "NoTest" => {
                datom_codec::Headed::nothing(v)?;
                std::result::Result::Ok(Self::NoTest)
            }
            "RunHermeticTest" => {
                std::result::Result::Ok(Self::RunHermeticTest(datom_codec::Carrying::body(v)?))
            }
            _ => std::result::Result::Err(datom_codec::Headed::reject(
                &v,
                datom_codec::Problem::UnknownVariant(
                    protos::Word::try_from(v.name).expect("variant name"),
                ),
            )),
        }
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapTestPlan {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            match self {
                Self::NoTest => datom_codec::Datom::Word(
                    datom_codec::DatomWord::try_from(
                        protos::Word::try_from("NoTest").expect("static variant"),
                    )
                    .expect("stable variant"),
                ),
                Self::RunHermeticTest(p0) => datom_codec::Datom::Variant(
                    protos::Symbol::try_from("RunHermeticTest").expect("static variant"),
                    std::boxed::Box::new(
                        protos::Conceivable::conceive(p0)
                            .expect("infallible datom ascent")
                            .1,
                    ),
                ),
            },
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapHermeticTest(
    pub BootstrapFlakeReference,
    pub BootstrapNixSystem,
    pub BootstrapOutputSelector,
);
impl datom_codec::Datomic for BootstrapHermeticTest {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let mut p = datom_codec::Sited::positions(site, 3)?;
        let p0: BootstrapFlakeReference = datom_codec::Positional::position(&mut p)?;
        let p1: BootstrapNixSystem = datom_codec::Positional::position(&mut p)?;
        let p2: BootstrapOutputSelector = datom_codec::Positional::position(&mut p)?;
        std::result::Result::Ok(Self(p0, p1, p2))
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapHermeticTest {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            datom_codec::Datom::Struct(vec![
                protos::Conceivable::conceive(&self.0)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.1)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.2)
                    .expect("infallible datom ascent")
                    .1,
            ]),
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapActivationBackend {
    RemoteNixosSystemdBootV1(BootstrapRemoteNixosSystemdBootV1),
    LocalBootstrapV1(BootstrapLocalBootstrapV1),
}
impl datom_codec::Datomic for BootstrapActivationBackend {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let v = datom_codec::Sited::variant(site)?;
        match v.name {
            "RemoteNixosSystemdBootV1" => std::result::Result::Ok(Self::RemoteNixosSystemdBootV1(
                datom_codec::Carrying::body(v)?,
            )),
            "LocalBootstrapV1" => {
                std::result::Result::Ok(Self::LocalBootstrapV1(datom_codec::Carrying::body(v)?))
            }
            _ => std::result::Result::Err(datom_codec::Headed::reject(
                &v,
                datom_codec::Problem::UnknownVariant(
                    protos::Word::try_from(v.name).expect("variant name"),
                ),
            )),
        }
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapActivationBackend {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            match self {
                Self::RemoteNixosSystemdBootV1(p0) => datom_codec::Datom::Variant(
                    protos::Symbol::try_from("RemoteNixosSystemdBootV1").expect("static variant"),
                    std::boxed::Box::new(
                        protos::Conceivable::conceive(p0)
                            .expect("infallible datom ascent")
                            .1,
                    ),
                ),
                Self::LocalBootstrapV1(p0) => datom_codec::Datom::Variant(
                    protos::Symbol::try_from("LocalBootstrapV1").expect("static variant"),
                    std::boxed::Box::new(
                        protos::Conceivable::conceive(p0)
                            .expect("infallible datom ascent")
                            .1,
                    ),
                ),
            },
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapRemoteNixosSystemdBootV1(
    pub BootstrapNixStoreUri,
    pub BootstrapSshDestination,
    pub BootstrapSshPolicy,
    pub BootstrapSystemProfilePath,
    pub BootstrapBootEntriesDirectory,
);
impl datom_codec::Datomic for BootstrapRemoteNixosSystemdBootV1 {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let mut p = datom_codec::Sited::positions(site, 5)?;
        let p0: BootstrapNixStoreUri = datom_codec::Positional::position(&mut p)?;
        let p1: BootstrapSshDestination = datom_codec::Positional::position(&mut p)?;
        let p2: BootstrapSshPolicy = datom_codec::Positional::position(&mut p)?;
        let p3: BootstrapSystemProfilePath = datom_codec::Positional::position(&mut p)?;
        let p4: BootstrapBootEntriesDirectory = datom_codec::Positional::position(&mut p)?;
        std::result::Result::Ok(Self(p0, p1, p2, p3, p4))
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapRemoteNixosSystemdBootV1 {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            datom_codec::Datom::Struct(vec![
                protos::Conceivable::conceive(&self.0)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.1)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.2)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.3)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.4)
                    .expect("infallible datom ascent")
                    .1,
            ]),
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapSshPolicy(
    pub BootstrapSshIdentityFile,
    pub BootstrapSshKnownHostsFile,
    pub BootstrapStrictHostKeyMode,
);
impl datom_codec::Datomic for BootstrapSshPolicy {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let mut p = datom_codec::Sited::positions(site, 3)?;
        let p0: BootstrapSshIdentityFile = datom_codec::Positional::position(&mut p)?;
        let p1: BootstrapSshKnownHostsFile = datom_codec::Positional::position(&mut p)?;
        let p2: BootstrapStrictHostKeyMode = datom_codec::Positional::position(&mut p)?;
        std::result::Result::Ok(Self(p0, p1, p2))
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapSshPolicy {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            datom_codec::Datom::Struct(vec![
                protos::Conceivable::conceive(&self.0)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.1)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.2)
                    .expect("infallible datom ascent")
                    .1,
            ]),
        ))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapStrictHostKeyMode {
    RequireKnownHost,
}
impl datom_codec::Datomic for BootstrapStrictHostKeyMode {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let v = datom_codec::Sited::variant(site)?;
        match v.name {
            "RequireKnownHost" => {
                datom_codec::Headed::nothing(v)?;
                std::result::Result::Ok(Self::RequireKnownHost)
            }
            _ => std::result::Result::Err(datom_codec::Headed::reject(
                &v,
                datom_codec::Problem::UnknownVariant(
                    protos::Word::try_from(v.name).expect("variant name"),
                ),
            )),
        }
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapStrictHostKeyMode {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            match self {
                Self::RequireKnownHost => datom_codec::Datom::Word(
                    datom_codec::DatomWord::try_from(
                        protos::Word::try_from("RequireKnownHost").expect("static variant"),
                    )
                    .expect("stable variant"),
                ),
            },
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapLocalBootstrapV1(
    pub BootstrapSystemProfilePath,
    pub BootstrapBootEntriesDirectory,
);
impl datom_codec::Datomic for BootstrapLocalBootstrapV1 {
    fn incorporate(site: datom_codec::Site<'_>) -> std::result::Result<Self, datom_codec::Fault> {
        let mut p = datom_codec::Sited::positions(site, 2)?;
        let p0: BootstrapSystemProfilePath = datom_codec::Positional::position(&mut p)?;
        let p1: BootstrapBootEntriesDirectory = datom_codec::Positional::position(&mut p)?;
        std::result::Result::Ok(Self(p0, p1))
    }
}
impl protos::Conceivable<datom_codec::Datom> for BootstrapLocalBootstrapV1 {
    type Fault = std::convert::Infallible;
    fn conceive(&self) -> std::result::Result<protos::Situated<datom_codec::Datom>, Self::Fault> {
        std::result::Result::Ok(protos::Situated(
            protos::Situation {
                extent: protos::Extent(0, 0),
                children: vec![],
            },
            datom_codec::Datom::Struct(vec![
                protos::Conceivable::conceive(&self.0)
                    .expect("infallible datom ascent")
                    .1,
                protos::Conceivable::conceive(&self.1)
                    .expect("infallible datom ascent")
                    .1,
            ]),
        ))
    }
}
pub type BootstrapFlakeReference = protos::Text;
pub type BootstrapNixSystem = protos::Text;
pub type BootstrapOutputSelector = protos::Text;
pub type BootstrapProposalSource = protos::Text;
pub type BootstrapClusterName = protos::Text;
pub type BootstrapNodeName = protos::Text;
pub type BootstrapSecretsDirectory = protos::Text;
pub type BootstrapBuilderSpec = protos::Text;
pub type BootstrapJournalParent = protos::Text;
pub type BootstrapGcRootPath = protos::Text;
pub type BootstrapTerminalEvidencePath = protos::Text;
pub type BootstrapNixStoreUri = protos::Text;
pub type BootstrapSshDestination = protos::Text;
pub type BootstrapSshIdentityFile = protos::Text;
pub type BootstrapSshKnownHostsFile = protos::Text;
pub type BootstrapSystemProfilePath = protos::Text;
pub type BootstrapBootEntriesDirectory = protos::Text;
