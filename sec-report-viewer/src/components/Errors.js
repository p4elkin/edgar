import {cellWithTwoValues, FilingGrid, formatMetric, formatNumber} from "./table";
import React, {useState} from "react";

export const MissingRevenueList = () => {

    const fetchIdRef = React.useRef(0);
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(false);

    const fetchData = async function({ pageSize, pageIndex }) {
        // Give this fetch an ID
        const fetchId = ++fetchIdRef.current

        // Set the loading state
        setLoading(true);
        if (fetchId === fetchIdRef.current) {
            let url = new URL(`http://${window.location.hostname}:8888/withErrors`), params = {
                type: "revenue",
                limit: pageSize,
                offset: pageIndex * pageSize,
            };

            // TODO - extract function
            Object.keys(params).forEach(key => {
                if ((params[key] && params[key] !== "") || params[key] === 0) url.searchParams.append(key, params[key])
            })

            let fetchedFilings = await fetch(url);

            setData(await fetchedFilings.json());
            setLoading(false)
        }
    };

    return <FilingGrid columns={configureColumns()} data={data} fetchData={fetchData} loading={loading}/>;
}

const configureColumns = () => [
    {
        Header: 'With no revenue',
        columns: [
            {
                Header: 'Company',
                accessor: row => ({name: row.name, ticker: row.ticker}),
                Cell: ({value}) => cellWithTwoValues(value.name, value.ticker)
            },
            {
                Header: 'Current EPS',
                accessor: row => formatNumber(row.eps),
            },
            {
                Header: 'Revenue ($ millions)',
                accessor: row => formatNumber(row.revenue),
            },
            {
                Header: 'Net Income ($ millions)',
                accessor: row => formatNumber(row.netIncome),
            }

        ],
    }
];
