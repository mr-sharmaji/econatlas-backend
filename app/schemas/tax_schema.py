from datetime import datetime

from pydantic import BaseModel, Field


class TaxFinancialYearResponse(BaseModel):
    id: str
    label: str


class TaxRoundingPolicyResponse(BaseModel):
    currency_scale: int = 2
    percentage_scale: int = 2
    tax_rounding: str = "nearest_rupee"


class TaxSlabResponse(BaseModel):
    upper_limit: float
    rate: float


class IncomeTaxRebateRuleResponse(BaseModel):
    threshold: float
    max_rebate: float
    resident_only: bool = True
    marginal_relief: bool = False


class IncomeTaxSurchargeRuleResponse(BaseModel):
    threshold: float
    rate: float


class IncomeTaxRulesResponse(BaseModel):
    standard_deduction: dict[str, float]
    old_basic_exemption: dict[str, float]
    old_slabs: list[TaxSlabResponse]
    new_slabs: list[TaxSlabResponse]
    rebate: dict[str, IncomeTaxRebateRuleResponse]
    surcharge: dict[str, list[IncomeTaxSurchargeRuleResponse]]
    cess_rate: float


class CapitalGainsAssetRuleResponse(BaseModel):
    holding_period_months: int
    stcg_rate: float
    ltcg_rate: float
    ltcg_exemption: float = 0.0
    section: str
    stcg_mode: str = "fixed"
    ltcg_mode: str = "fixed"
    always_short_term: bool = False
    note: str = ""


class CapitalGainsRulesResponse(BaseModel):
    assets: dict[str, CapitalGainsAssetRuleResponse]


class AdvanceTaxInstallmentResponse(BaseModel):
    label: str
    due_date: str
    cumulative_percent: float


class AdvanceTaxRulesResponse(BaseModel):
    installments: list[AdvanceTaxInstallmentResponse]
    interest_rate_234c: float
    interest_rate_234b: float = 0.01
    interest_threshold: float = 10000


class TdsSectionRuleResponse(BaseModel):
    section: str
    label: str
    rate: float
    threshold: float
    resident_only: bool = True


class TdsSubTypeRuleResponse(BaseModel):
    value: str
    label: str
    rate_individual: float
    rate_other: float
    rate_no_pan: float


class TdsPaymentTypeRuleResponse(BaseModel):
    value: str
    section_code: str
    label: str
    description: str
    threshold: float = 0.0
    threshold_individual: float | None = None
    threshold_other: float | None = None
    always_apply: bool = False
    rate_individual: float
    rate_other: float
    rate_no_pan: float
    sub_type_options: list[TdsSubTypeRuleResponse] = Field(default_factory=list)


class TdsDefaultsResponse(BaseModel):
    pan: str = "yes"
    recipient: str = "individual"
    fees194j: str = "others"


class TdsRulesResponse(BaseModel):
    sections: list[TdsSectionRuleResponse] = Field(default_factory=list)
    payment_types: list[TdsPaymentTypeRuleResponse] = Field(default_factory=list)
    defaults: TdsDefaultsResponse = Field(default_factory=TdsDefaultsResponse)


class TaxRuleSetResponse(BaseModel):
    income_tax: IncomeTaxRulesResponse
    capital_gains: CapitalGainsRulesResponse
    advance_tax: AdvanceTaxRulesResponse
    tds: TdsRulesResponse


class TaxHelperPointsResponse(BaseModel):
    hub: list[str]
    income_tax: list[str]
    capital_gains: list[str]
    advance_tax: list[str]
    tds: list[str]


class TaxConfigResponse(BaseModel):
    version: str
    hash: str
    supported_fy: list[TaxFinancialYearResponse]
    default_fy: str
    last_synced_at: datetime | None = None
    disclaimer: str
    helper_points: TaxHelperPointsResponse
    rounding_policy: TaxRoundingPolicyResponse
    rules_by_fy: dict[str, TaxRuleSetResponse]
