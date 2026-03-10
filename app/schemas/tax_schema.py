from datetime import datetime

from pydantic import BaseModel


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


class CapitalGainsRulesResponse(BaseModel):
    assets: dict[str, CapitalGainsAssetRuleResponse]


class AdvanceTaxInstallmentResponse(BaseModel):
    label: str
    due_date: str
    cumulative_percent: float


class AdvanceTaxRulesResponse(BaseModel):
    installments: list[AdvanceTaxInstallmentResponse]
    interest_rate_234c: float


class TdsSectionRuleResponse(BaseModel):
    section: str
    label: str
    rate: float
    threshold: float
    resident_only: bool = True


class TdsRulesResponse(BaseModel):
    sections: list[TdsSectionRuleResponse]


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
