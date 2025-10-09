# API Rate Limiting Optimization

## Overview
Optimized API rate limiting across all extractors to improve processing speed while respecting Alpha Vantage API limits.

## Changes Made

### **Company Overview Extractor**
- **Previous**: Fixed 0.8s delay (75 calls/minute theoretical max)
- **New**: Configurable 0.3s delay (200 calls/minute max)
- **Environment Variable**: `API_DELAY_SECONDS=0.3`

### **Time Series Extractor** 
- **Previous**: Fixed delay based on calls_per_minute parameter
- **New**: Configurable delay via environment variable
- **Environment Variable**: `API_DELAY_SECONDS=0.3`

### **Balance Sheet Extractor**
- **Status**: Uses same approach as Company Overview
- **Default**: 0.3s delay (optimized)

## Performance Impact

### **Before Optimization:**
- Company Overview: ~22 symbols/minute (2.73s per symbol)
- Theoretical max with 0.8s delay: 75 calls/minute
- Actual performance: Much slower than theoretical

### **After Optimization:**
- **Target**: 200 calls/minute theoretical max with 0.3s delay
- **Expected**: 60-120 symbols/minute (realistic with API response times)
- **Improvement**: 3-5x faster processing

## Configuration Options

### **Default (Optimized)**
```bash
# No environment variable needed - uses optimized 0.3s default
```

### **Conservative (Original)**
```bash
export API_DELAY_SECONDS=0.8  # Original conservative rate
```

### **Aggressive (Use with caution)**
```bash
export API_DELAY_SECONDS=0.1  # Very fast - monitor for rate limit errors
```

### **Custom Rate**
```bash
export API_DELAY_SECONDS=0.5  # Custom delay in seconds
```

## Alpha Vantage API Limits

### **Free Tier**: 25 calls/minute
### **Premium Tier**: 75+ calls/minute (your tier)
### **Enterprise**: Higher limits

## Safety Features

### **Built-in Protection:**
- ✅ Rate limit detection and backoff
- ✅ Retry logic for temporary failures  
- ✅ Configurable via environment variables
- ✅ Logging shows actual rate being used

### **Monitoring:**
```
API delay: 0.3s (200.0 calls/minute max)
```

## Recommendations

### **For Development/Testing:**
```bash
export API_DELAY_SECONDS=0.3  # Good balance of speed and safety
```

### **For Production:**
```bash
export API_DELAY_SECONDS=0.5  # More conservative for reliability
```

### **For Maximum Speed:**
```bash
export API_DELAY_SECONDS=0.2  # Fast but monitor for rate limit errors
```

## Expected Results

With the 0.3s delay optimization:
- **Company Overview**: Should process 60-120 symbols/minute (vs previous 22/minute)
- **Time Series**: Should maintain high throughput with better control
- **Overall**: 3-5x improvement in extraction speed

## Fallback Plan

If rate limiting issues occur:
1. Increase `API_DELAY_SECONDS` to 0.5 or 0.8
2. Monitor logs for rate limit warnings
3. Adjust based on actual API response performance

The extractors will automatically handle rate limit responses and backoff as needed.