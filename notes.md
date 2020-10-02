
# TABLE
   | Restrictions on | Category                         | Variable                           | TABLE                      | Type     | Values     |
   |-----------------|----------------------------------|------------------------------------|----------------------------|----------|------------|
   | Payouts         | 1. Dividend pmnt. restrs.        | DIVIDENDS_RELATED_PAYMENTS_IS      | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 1. Dividend pmnt. restrs.        | DIVIDENDS_RELATED_PAYMENTS_SUB     | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | -------------------------------- | ---------------------------------- | -------------------------- | -------- | ---------- |
   |                 | 2. Share repurchase restrs       | RESTRICTED_PAYMENTS                | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 2. Share repurchase restrs       | ECONOMIC_COV_DEF                   | BONDHOLDER PROTECTIVE      | Bool     |            |
   |                 | 2. Share repurchase restrs       | COVENANT_DEFEAS_WO_TAX_CONSEQ      | BONDHOLDER PROTECTIVE      | Bool     |            |
   |                 | 2. Share repurchase restrs       | LEGAL_DEFEASANCE                   | BONDHOLDER PROTECTIVE      | Bool     |            |
   |                 | 2. Share repurchase restrs       | DEFEASANCE_WO_TAX_CONSEQ           | BONDHOLDER PROTECTIVE      | Bool     |            |
   |-----------------|----------------------------------|------------------------------------|----------------------------|----------|------------|
   | Financing Act.  | 3. Funded debt restrs            | FUNDED_DEBT_IS                     | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 3. Funded debt restrs            | FUNDED_DEBT_SUB                    | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | ------------------------------   | --------------------------------   | ------------------------   | ------   | --------   |
   |                 | 4. Subordinate debt restrs       | SUBORDINATED_DEBT_ISSUANCE         | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | ------------------------------   | --------------------------------   | ------------------------   | ------   | --------   |
   |                 | 5. Senior debt restrs            | SENIOR_DEBT_ISSUANCE               | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | ------------------------------   | --------------------------------   | ------------------------   | ------   | --------   |
   |                 | 6. Secured debt restrs           | NEGATIVE_PLEDGE_COVENANT           | BONDHOLDER PROTECTIVE      | Bool     |            |
   |                 | 6. Secured debt restrs           | SUBSIDIARY_GUARANTEE               | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | ------------------------------   | --------------------------------   | ------------------------   | ------   | --------   |
   |                 | 7. Total leverage test           | MAINTENANCE_NET_WORTH              | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 7. Total leverage test           | INDEBTEDNESS_IS                    | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 7. Total leverage test           | FIXED_CHARGE_COVERAGE_IS           | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 7. Total leverage test           | LEVERAGE_TEST_IS                   | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 7. Total leverage test           | NET_EARNINGS_TEST_ISSUANCE         | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 7. Total leverage test           | BORROWING_RESTRICTED               | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | 7. Total leverage test           | INDEBTEDNESS_SUB                   | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | 7. Total leverage test           | FIXED_CHARGE_COVERAGE_SUB          | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | 7. Total leverage test           | LEVERAGE_TEST_SUB                  | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | ------------------------------   | --------------------------------   | ------------------------   | ------   | --------   |
   |                 | 8. Sale & leaseback              | SALES_LEASEBACK_IS                 | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 8. Sale & leaseback              | SALES_LEASEBACK_SUB                | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | ------------------------------   | --------------------------------   | ------------------------   | ------   | --------   |
   |                 | 9. Stock issue restrs.           | STOCK_ISSUANCE_ISSUER              | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 9. Stock issue restrs.           | STOCK_ISSUANCE                     | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | 9. Stock issue restrs.           | PREFERRED_STOCK_ISSUANCE           | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | ------------------------------   | --------------------------------   | ------------------------   | ------   | --------   |
   |                 | 10. Rating & net wrth. trgs.     | RATING_DECLINE_TRIGGER_PUT         | BONDHOLDER PROTECTIVE      | Bool     |            |
   |                 | 10. Rating & net wrth. trgs.     | RATING_DECLINE_PROVISION           | BONDHOLDER PROTECTIVE      | ?        |            |
   |                 | 10. Rating & net wrth. trgs.     | DECLINING_NET_WORTH                | BONDHOLDER PROTECTIVE      | Bool     |            |
   |                 | 10. Rating & net wrth. trgs.     | DECLINING_NET_WORTH_TRIGGER        | BONDHOLDER PROTECTIVE      | ?        |            |
   |                 | 10. Rating & net wrth. trgs.     | DECLINING_NET_WORTH_PERCENTAGE     | BONDHOLDER PROTECTIVE      | ?        |            |
   |                 | 10. Rating & net wrth. trgs.     | DECLINING_NET_WORTH_PROVISIONS     | BONDHOLDER PROTECTIVE      | ?        |            |
   |                 | ------------------------------   | --------------------------------   | ------------------------   | ------   | --------   |
   |                 | 11. Cross-default provisions     | CROSS_DEFAULT                      | BONDHOLDER PROTECTIVE      | Bool     |            |
   |                 | 11. Cross-default provisions     | CROSS_ACCELERATION                 | BONDHOLDER PROTECTIVE      | Bool     |            |
   |                 | ------------------------------   | --------------------------------   | ------------------------   | ------   | --------   |
   |                 | 12. Poison put                   | CHANGE_CONTROL_PUT_PROVISIONS      | BONDHOLDER PROTECTIVE      | Bool     |            |
   |                 | 12. Poison put                   | VOTING_POWER_PERCENTAGE            | BONDHOLDER PROTECTIVE      | ?        |            |
   |                 | 12. Poison put                   | VOTING_POWER_PERCENTAGE_ERP        | BONDHOLDER PROTECTIVE      | ?        |            |
   |-----------------|----------------------------------|------------------------------------|----------------------------|----------|------------|
   | Investments     | 13. Asset sale clause            | ASSET_SALE_CLAUSE                  | BONDHOLDER PROTECTIVE      | Bool     |            |
   |                 | 13. Asset sale clause            | SALE_ASSETS                        | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 13. Asset sale clause            | LIENS_IS                           | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 13. Asset sale clause            | AFTER_ACQUIRED_PROPERTY_CLAUSE     | BONDHOLDER PROTECTIVE      | Bool     |            |
   |                 | 13. Asset sale clause            | SALE_XFER_ASSETS_UNRESTRICTED      | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | 13. Asset sale clause            | LIENS_SUB                          | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | ------------------------------   | --------------------------------   | ------------------------   | ------   | --------   |
   |                 | 14. Invest. policy restrs.       | INVESTMENTS                        | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 14. Invest. policy restrs.       | TRANSACTION_AFFILIATES             | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 14. Invest. policy restrs.       | STOCK_TRANSFER_SALE_DISP           | ISSUER RESTRICTIVE         | Bool     |            |
   |                 | 14. Invest. policy restrs.       | INVESTMENTS_UNRESTRICTED_SUBS      | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | 14. Invest. policy restrs.       | SUBSIDIARY_REDESIGNATION           | SUBSIDIARY RESTRICTIVE     | Bool     |            |
   |                 | ------------------------------   | --------------------------------   | ------------------------   | ------   | --------   |
   |                 | 15. Merger restrictions          | CONSOLIDATION_MERGER               | ISSUER RESTRICTIVE         | Bool     |            |
   |-----------------|----------------------------------|------------------------------------|----------------------------|----------|------------|

# Covenants Categories

## Restrictions on Payouts to equity holders and others
The first two categories restrict payouts to equity holders and others. 

### 1. Dividend pmnt. restrs. (DONE)
An issue has a dividend restriction if there is a covenant limiting the
dividend payments of the issuer or a subsidiary of the issuer. Typical
subsidiary restrictions limit dividend payments to the parent, thereby
preventing the parent from draining the subsidiary’s assets. 

### 2. Share repurchase restrs
An issue has a share repurchase restriction if there is a covenant limiting the
issuer’s freedom to make payments (other than dividend payments) to
shareholders and others. Note that this covenant would also restrict the
issuers’ ability to redeem subordinate debt.

## Restrictions on Financing Activities
The next seven categories place restrictions on financing activities.

### 3. Funded debt restrs  (DONE)
A funded debt restriction prevents the issuer and/or subsidiary from issuing
additional debt with a maturity of 1 year or longer.

### 4. Subordinate debt restrs  (DONE)
Restricts the issuer from issuing additional subordinate debt.

### 5. Senior debt restrs  (DONE)
Restricts the issuer from issuing additional senior debt.

### 6. Secured debt restrs
Restricts the issuer from issuing additional secured debt. Note that the
secured debt covenant is referred to as a negative pledge, and typically
specifies that the issuer cannot issue secured debt unless it secures the
current issue on a pari passu basis. 

### 7. Total leverage test
The category of covenants that we refer to as “total leverage tests” includes
a variety of accounting-based restrictions on leverage, ranging from
a requirement that the issuer maintain a specified minimum net worth to
a requirement that the issuer maintain a specified minimum ratio of earnings
to fixed charges. 

### 8. Sale & leaseback  (DONE)
A sale and leaseback covenant restricts the issuer and/or
subsidiary from selling and then leasing back assets that provide security for
the debtholder. This provision usually requires that the proceeds from the sale
be used to retire debt or acquire substantially equivalent property. 

### 9. Stock issue restrs.
Finally, the stock issue restriction restricts the issuer and/or subsidiary
from issuing additional common or preferred stock.

## Event-Driven Covenants
The next three categories are event-driven covenants. 

### 10. Rating & net wrth. trgs. 
An issue has a rating or net worth trigger if certain provisions are triggered
(e.g., a put option) when either the credit rating or net worth of the issuer
falls below a specified level. 

### 11. Cross-default provisions
An issue has a cross-default provision if default (or acceleration of payments
in default) is triggered in the issue when default (or acceleration of payments
in default) occurs for any other debt issue.

### 12. Poison put
The poison put provision is included as a separate category, since it is
triggered in the event of a change in control.

## Restrictions on Investment Policy
The remaining three covenant categories place restrictions on investment
policy. 

### 13. Asset sale clause (DONE)
An issue has an asset sale clause if the issuer and/or subsidiary are required
to use the net proceeds from the sale of certain assets to redeem the issue at
par or at a premium to par. 

### 14. Invest. policy restrs.
Investment policy restrictions proscribe certain risky investments for the
issuer and/or subsidiary.

### 15. Merger restrictions (DONE)
Finally, a merger restriction typically specifies that the surviving entity
must assume the debt and abide by all of the covenants in the debt.

# Variables Wiki

| DATASET | Name | Type | Description | Depends on |
|---------|------|------|-------------|------------|
| TRACE   |

# MERGENT BOND ISSUE

maybe consider analyzing EXCHANGE_LISTING

## BONDHOLDER_PROTECTIVE

1.  :ISSUE_ID,
2.  :NEGATIVE_PLEDGE_COVENANT,
    - The issuer cannot issue secured debt unless it secures the current issue on a pari passu basis.
3.  :COVENANT_DEFEAS_WO_TAX,
    - Gives the issuer the right to defease indenture covenants without tax
        consequences for bondholders. If exercised, this would free the issuer from
        covenants set forth in the indenture or prospectus, but leaves them liable for
        the remaining debt. The issuer must also set forth an opinion of counsel that
        states bondholders will not recognize income for federal tax purposes as a
        result of the defeasance.
4.  :LEGAL_DEFEASANCE,
    - Gives the issuer the right to defease the monetary portion of the security.
        Legal defeasance occurs when the issuer places in an escrow account an amount of
        money or US government securities sufficient to match the remaining interest and
        principle payments of the current issue. This removes the debt from the issuers’
        balance sheet, but leaves the borrower still liable for covenants set forth
        under the indenture. This type of defeasance may have tax consequences for
        bondholders.
5.  :DEFEASANCE_WO_TAX_CONSEQ,
    - Gives the issuer the right to defease the monetary portion of the security
        without tax consequence for bondholders. This type of defeasance occurs when the
        issuer places in an escrow account an amount of money or US government
        securities sufficient to match the remaining interest and principle payments of
        the current issue. This removes the debt from the issuers’ balance sheet, but
        leaves the borrower still liable for covenants set forth under the indenture.
        The issuer must also set forth an opinion of counsel that states bondholders
        will not recognize income for federal tax purposes as a result of the
        defeasance. cross_default
6.  :CROSS_DEFAULT,
    - A bondholder protective covenant that will activate an event of default in their issue, if an event of default has occurred under any other debt of the company.
7.  :CROSS_ACCELERATION
    - A bondholder protective covenant that allows the holder to accelerate their debt, if any other debt of the organization has been accelerated due to an event of default.
8.  :CHANGE_CONTROL_PUT_PROVISIONS,
    - Upon a change of control in the issuer, bondholders have the option of selling
        the issue back to the issuer (poison put). Other conditions may limit the
        bondholder’s ability to exercise the put option. Poison puts are often used when
        a company fears an unwanted takeover by ensuring that a successful hostile
        takeover bid will trigger an event that substantially reduces the value of the
        company.
9.  :VOTING_POWER_PERCENTAGE,
10.  :VOTING_POWER_PERCENTAGE_ERP,
11.  :RATING_DECLINE_TRIGGER_PUT,
    - A decline in the credit rating of the issuer (or issue) triggers a bondholder put provision.
12.  :RATING_DECLINE_PROVISION,
13.  :DECLINING_NET_WORTH,
    - If issuer’s net worth (as defined) falls below minimum level, certain bond provisions are triggered.
14.  :DECLINING_NET_WORTH_TRIGGER,
15.  :DECLINING_NET_WORTH_PERCENTAGE,
16.  :DECLINING_NET_WORTH_PROVISIONS,
17.  :AFTER_ACQUIRED_PROPERTY_CLAUSE,
    - Property acquired after the sale of current debt issues will be included in
        the current issuer’s mortgage. Normally found in utility issuers with
        blanket mortgages.
18.  :ECONOMIC_COV_DEF,
    - Gives the issuer the right to defease indenture covenants. If exercised,
      this would free the issuer from covenants set forth in the indenture or
      prospectus, but leaves them liable for the remaining debt. This type of
      defeasance may have tax consequences for bondholders.
19.  :ASSET_SALE_CLAUSE,
    - Covenant requiring the issuer to use net proceeds from the sale of certain
      assets to redeem the bonds at par or at a premium. This covenant does not limit
      the issuers' right to sell assets.

## ISSUE

### ISSUE - Main

1.  ISSUE_ID,
2.  ISSUER_ID
3.  ISSUER_CUSIP
4.  ISSUE_CUSIP
    - The seventh through ninth digits of the unique nine-digit alphanumeric code
        assigned to each issue by the Committee on Uniform Securities Identification
        Procedures. The first six characters of CUSIP specify the issuer: characters
        seven and eight specify the issue, and the ninth digit is the check digit. If an
        issue has not yet been assigned a CUSIP, FTSE Russell creates an estimated CUSIP
        for temporary use using one of the following symbols: @ ? $ ! & #.
5.  MATURITY
6.  SECURITY_LEVEL
    - Indicates if the security is a secured, senior or subordinated issue of the issuer.
7.  SECURITY_PLEDGE
    - A flag indicating that certain assets have been pledged as security for the issue.
8.  ENHANCEMENT
    - Flag indicating that the issue has credit enhancements (e.g., guarantees, letter of credit, etc.).
9.  CONVERTIBLE
    - Flag indicating the issue can be converted to the common stock (or other security) of the issuer. Further information can be found in the CONVERTIBLE table.
10. ASSET_BACKED
    - Flag indicating that the issue is an asset backed issue that is collateralized by a portfolio of loans or assets other than single family mortgages.
11. ISSUE_OFFERED_GLOBAL
12. GROSS_SPREAD
    - The difference between the price that the issuer receives for its securities and the price that investors pay for them. This spread equals the selling concession plus the underwriting and management fees.
13. RULE_144a
    - A flag denoting that the issue is a private placement exempt from registration under SEC Rule 144a. Rule 144a issues are generally offered to a limited number of institutional investors, known as QIB’s (Qualified Institutional Buyers).
14. OFFERING_AMT
15. PRINCIPAL_AMT
16. COVENANTS
    - Flag indicating that the issue’s covenants are recorded in the COVENANTS table.
17. DEFEASANCE_TYPE
    - A code indicating the type of defeasance allowed (e.g., covenants or legal). Covenant defeasance removes the restrictions of indenture covenants, but leaves the issuer liable for the remaining debt. Legal defeasance removes the issue from the issuer’s balance sheet but leaves the issuer liable for any covenants specified in the indenture.
18. REDEEMABLE
    - A flag indicating that the bond is redeemable under certain circumstances.
19. REFUND_PROTECTION
    - A flag denoting that the issuer is restricted from refunding this issue. An issue is refunded when the issuer redeems the issue from proceeds of a second bond issue.
20. PUTABLE
    - Put option flag. A put option provides the bondholder with the option, but not the obligation, to sell the security back to the issuer at a specified price and time, under certain circumstances.
21. ACTIVE_ISSUE
    - Flag indicating whether all or a portion of this issue remains outstanding. A value of “Y” indicates the issue currently has an amount outstanding greater than zero. A value of “N” indicates the issue currently has an amount outstanding of zero (i.e. the issue has been retired in full).
22. PRIVATE_PLACEMENT
    - Flag indicating that the issue is only being offered privately to selected individuals and institutions and not to the general public.
23. BOND_TYPE
    - A code denoting the type of issue (e.g. US Agency Debenture, US Corporate MTN, etc.).
24. PERPETUAL
25. EXCHANGEABLE
    - Flag indicating the issue can be converted to the common stock (or other security) of a subsidiary or affiliate of the issuer.
26. FUNGIBLE
    - Flag denoting securities that are, by virtue of their terms, equivalent, interchangeable or substitutable. Fungible issues may be “reopened” in the future, on one or more occasions, increasing the total amount outstanding of the issue.
27. REGISTRATION_RIGHTS
    - Indicates the issue contains a registration rights agreement whereby the issuer agrees to file a registration statement (or an exchange offer registration statement) within a specified period.
28. PREFERRED_SECURITY
    - Flag indicating this issue is a preferred security (e.g. MIPS, PIES, TOPRS, Preferred Stock, etc.).
29. PRINCIPAL_PROTECTED
    - Flag indicating this issue is a preferred security (e.g. MIPS, PIES, TOPRS, Preferred Stock, etc.).
30. PRINCIPAL_PROTECTED_PERCENTAGE

### ISSUE - Other

1.  PROSPECTUS_ISSUER_NAME
2.  ISSUE_NAME
3.  COUPON_TYPE
4.  MTN
5.  YANKEE
6.  CANADIAN
7.  OID
8.  FOREIGN_CURRENCY
9.  SLOB
10. SETTLEMENT_TYPE
11. SELLING_CONCESSION
12. REALLOWANCE
13. COMP_NEG_EXCH_DEAL
14. RULE_415_REG
15. SEC_REG_TYPE1
16. SEC_REG_TYPE2
17. TREASURY_SPREAD
18. TREASURY_MATURITY
19. OFFERING_DATE
20. OFFERING_PRICE
21. OFFERING_YIELD
22. DELIVERY_DATE
23. UNIT_DEAL
24. FORM_OF_OWN
25. DENOMINATION
26. DEFEASED
27. DEFEASED_DATE
28. DEFAULTED
29. TENDER_EXCH_OFFER
30. REFUNDING_DATE
31. OVERALLOTMENT_OPT
32. ANNOUNCED_CALL
33. DEP_ELIGIBILITY
34. SUBSEQUENT_DATA
35. PRESS_RELEASE
36. ISIN
37. SEDOL

# TRACE v.s. MERGENT FISD

Functions:

- for each execution date:
    - count the number of cusips of bonds outstanding
    - count by rating (HY, IG)

Now, need to investigate why COMPLETE_CUSIP and RATING_DATE are not sufficient to
uniquely identify entries in the MERGENT FISD DataFrames

Reasons:
1.  Different ratings (RATING_TYPE)
