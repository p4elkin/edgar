import React, {useEffect, useState} from 'react'
import {cellWithTwoValues, PagedGrid, formatMetric} from "./table";
import {useGlobalState} from "../state";
import {location} from "../commons";
import {FilingAmendmentPopup} from "./FilingPopup";
import {Icon} from "@material-ui/core";
import {Launch} from "@material-ui/icons";
import Link from "@material-ui/core/Link";

export const Filings = () => {
    const [amendedFiling, setAmendedFiling] = useState(null);
    const columns = React.useMemo(() => configureColumns(setAmendedFiling), []);
    const [{filter}] = useGlobalState()

    const grid = <PagedGrid endPoint={`${location}/latestFilings`} filter={filter} columns={columns}/>
    if (amendedFiling) {
        return (<>
            {grid}
            <FilingAmendmentPopup id={amendedFiling.id} filing={amendedFiling} onClose={() => setAmendedFiling(null)}/>
        </>)
    } else {
        return (grid)
    }
}

const configureColumns = (setAmendedFiling) => [
            {
                Header: 'Company',
                accessor: row => row,
                Cell: ({value}) =>
                    (<>
                    <Link onClick={() => setAmendedFiling(value)}><Launch/></Link>
                    {cellWithTwoValues(value.name, value.ticker)}
                    </>)
            },
            {
                Header: 'Date',
                accessor: 'date',
            },
            {
                Header: 'Report type',
                accessor: 'type',
            },
            {
                Header: 'Current EPS',
                accessor: row => formatMetric(row.eps, row.epsYY),
            },
            {
                Header: 'Revenue ($ millions)',
                accessor: row => row,
                Cell: ({value}) => <span>{formatMetric(value.revenue, value.revenueYY)}</span>
            },
            {
                Header: 'Net Income ($ millions)',
                accessor: row => formatMetric(row.netIncome, row.netIncomeYY),
            },
            {
                Header: 'Latest yearly revenue ($ millions)',
                accessor: 'latestAnnualRevenue',
            },

            {
                Header: 'Filing',
                accessor: 'interactiveData',
                Cell: ({value}) => <a href={value}><Launch/></a>,
            }
];
