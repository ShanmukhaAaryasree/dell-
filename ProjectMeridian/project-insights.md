# Project Meridian Insight Summary

## Shipment quality

The mini dataset contained 141 shipment rows. Keeping the latest version per shipment ID removed 8 older versions. Of the remaining 133 rows, 122 passed validation and 11 were rejected.

## Findings and actions

| Finding | Recommended action |
|---|---|
| **63 of 78 delivered shipments arrived on time (80.77%).** The other 15 missed their promised date. | Review late shipments weekly and assign a follow-up owner to recurring carrier or lane issues. Track the delivered shipment count alongside the rate. |
| **Konkan Carriers delivered 3 of 5 shipments on time (60%), 22 percentage points below its 82% SLA.** | Review the two missed deliveries with the carrier and monitor a larger sample before changing shipment allocations. |
| **Weather and traffic accounted for 11 of the 15 late deliveries.** Weather was recorded for 7 and traffic congestion for 4. | Pilot weather alerts and earlier dispatch on lanes with repeat disruption. Compare on-time performance before and after the change. |

Figures were recomputed from the supplied mini dataset using the notebook's shipment validation and KPI rules. The carrier results reflect a small replay dataset.
