import React, {useEffect, useState} from "react";
import {location} from "../commons";
import {ResponsiveContainer, BarChart, CartesianGrid, Legend, Bar, Tooltip, XAxis, YAxis } from "recharts";
import {StockChart} from "./StockChart";
import styled from "styled-components";

const metrics = ["revenue", "netIncome", "cashIncome", "liabilities"];
const palette = {
    "revenue":"#57E7B8",
    "netIncome":"#337662",
    "cashIncome": "#425947",
    "liabilities": "#513C2C",
    "eps": "#3DD6D0"
};

export const CompanyInfo = ({ticker}) => {
    const [data, setData] = useState([])
    const [displayedCashMetrics, setDisplayedCashMetrics] = useState({revenue: true, netIncome: true, cashIncome: true, liabilities: true})

    async function loadData() {
        let fetchedFilings = await fetch(`${location}/10k?ticker=${ticker}`);

        let json = await fetchedFilings.json()
        setData(json)
    }

    useEffect(() => {
        loadData()
    }, []);

    const toggleSeriesVisibility = (item) => {
        const prop = item.value
        const updated = {...displayedCashMetrics}
        updated[prop] = !displayedCashMetrics[prop]
        setDisplayedCashMetrics(updated)
    };
    // Industry code
    // Plot with charts
    return (<>
            <div style={{width: "50%", height: "500px"}}>
                <ResponsiveContainer height='100%' width='100%'>
                    <BarChart data={getSeries(data, metrics, displayedCashMetrics)}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="fiscalYear" />
                        <YAxis orientation={'left'}/>
                        <Tooltip/>
                        {metrics.map(metric => (
                            <Bar dataKey={metric} fill={palette[metric]}/>
                        ))}
                        <Legend onClick={toggleSeriesVisibility}/>
                    </BarChart>
                </ResponsiveContainer>
                <QuoteChart ticker={ticker}/>
            </div>
        </>
    );
}

const QuoteChart = ({ticker}) => {
    const [data, setData] = useState([])
    async function loadData() {
        let fetchedFilings = await fetch(`${location}/quotes?ticker=${ticker}`, {
            headers: {
                'Content-Type': 'application/json'
            }
        });

        let json = await fetchedFilings.json()
        if (json && json.chart) {
            const timestamps = json.chart.result["0"].timestamp
            const quote = json.chart.result["0"].indicators.quote["0"]

            let data = []
            for (let i = 0; i < timestamps.length; ++i) {
                let timestamp = timestamps[i]
                data.push({
                    date : new Date(timestamp * 1000),
                    open : +quote.open[i],
                    high : +quote.high[i],
                    low : quote.low[i],
                    close : +quote.close[i],
                    volume: +quote.volume[i]
                })
            }

            setData(data)
        } else {
            setData([])
        }
    }

    useEffect(() => {
        loadData()
    }, [ticker]);

    return (<>{ (data.length > 0) ? <StockChart data={data}/> : <span/>}</>)
}

const getSeries = (data, metrics, active) => {
    return data.filter(filing => filing.fiscalYear).map(filing => {
        const result = {
            fiscalYear: filing["fiscalYear"],
        };
        metrics.forEach(metric => {
            if (active[metric]) {
                result[metric] = formatNumber(filing[metric])
            } else {
                result[metric] = 0;
            }
        });
        return result
    });
}

export const formatNumber = (num) => {
    let number = Number.parseFloat(num);
    if (!Number.isNaN(number) && Number.isFinite(number)) {
        return number
    } else {
        return 0
    }
}

