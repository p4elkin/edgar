export const toPercents = (value) => {
    if (value === "NaN") {
        return "-"
    }

    return parseFloat(((parseFloat(value) - 1) * 100).toFixed(2))
};
