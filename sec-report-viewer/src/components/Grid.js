import React, {useEffect, useState} from 'react'
import {FilingGrid} from "./table";
import {useGlobalState} from "../state";


export const Filings = () => {
    const columns = React.useMemo(configureColumns, []);
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(false);
    const fetchIdRef = React.useRef(0);
    const [{filter}] = useGlobalState()

    const fetchData = async function({ pageSize, pageIndex }) {
        // Give this fetch an ID
        const fetchId = ++fetchIdRef.current

        // Set the loading state
        setLoading(true);
        if (fetchId === fetchIdRef.current) {
            let url = new URL("http://localhost:8888/latestFilings"), params = {
                limit: pageSize,
                offset: pageIndex * pageSize,
                startDate: filter.startDate,
                endDate: filter.endDate,
                company: filter.company,
                revenueThreshold: 1000000000
            };

            Object.keys(params).forEach(key => {
                if ((params[key] && params[key] !== "") || params[key] === 0) url.searchParams.append(key, params[key])
            })

            let fetchedFilings = await fetch(url);

            setData(await fetchedFilings.json());
            setLoading(false)
        }
    };

    return (<FilingGrid filter={filter} columns={columns} data={data} fetchData={fetchData} loading={loading}/>)
}

const filterNullQueryParams = params => {
    return Object.keys(params)
        .filter(param => params[param])
        .reduce((res, param) => {
            res[param] = params[param]
        }, {})
}


const cellWithTwoValues = (right, left) => {
    return (<>
        <span style={{float: "left"}}>{right}</span>
        <span style={{float: "right"}}>{left}</span>
    </>)
}

const metric = (right, left) => {
    const color = left > 0 ? "green" : "red"
    const increaseStr = left > 0 ? `+${left}` : left
    return (<>
        <span style={{float: "left"}}>{right}</span>
        <span style={{float: "right", color: color}}>({increaseStr}%)</span>
    </>)
}

const formatNumber = (num) => {
    return Number.parseFloat(num).toFixed(2)
}

const formatMetric = (value, yearToYearRatio) => {
    const numValue = Number.parseFloat(value)
    if (Number.isNaN(numValue)) {
        return (<span>-</span>)
    } else {
        const yearToYearInPercents = formatNumber((Number.parseFloat(yearToYearRatio) - 1.0) * 100.0)
        return metric(numValue.toFixed(2), `${yearToYearInPercents}`)
    }
}

const configureColumns = () => [
    {
        Header: 'Filings',
        columns: [
            {
                Header: 'Company',
                accessor: row => ({name: row.name, ticker: row.ticker}),
                Cell: ({value}) => cellWithTwoValues(value.name, value.ticker)
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
                accessor: row => formatMetric(row.revenue, row.revenueYY),
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
                Cell: ({value}) => <a href={value}>Filing</a>,
            }
        ],
    }
];
