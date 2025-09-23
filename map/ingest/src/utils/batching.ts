/**
 * Batching utilities for high-performance data processing
 */

export interface BatchProcessor<T> {
  processBatch(batch: T[]): Promise<void>;
}

export class BatchManager<T> {
  private batch: T[] = [];
  private readonly batchSize: number;
  private readonly processor: BatchProcessor<T>;
  private processedCount: number = 0;

  constructor(batchSize: number, processor: BatchProcessor<T>) {
    this.batchSize = batchSize;
    this.processor = processor;
  }

  async add(item: T): Promise<void> {
    this.batch.push(item);
    
    if (this.batch.length >= this.batchSize) {
      await this.flush();
    }
  }

  async flush(): Promise<void> {
    if (this.batch.length === 0) return;
    
    const currentBatch = [...this.batch];
    this.batch = [];
    
    await this.processor.processBatch(currentBatch);
    this.processedCount += currentBatch.length;
  }

  getProcessedCount(): number {
    return this.processedCount;
  }

  getPendingCount(): number {
    return this.batch.length;
  }
}

export class ProgressTracker {
  private startTime: Date;
  private lastUpdate: Date;
  private processed: number = 0;
  private readonly total?: number;
  private readonly updateInterval: number;

  constructor(total?: number, updateIntervalMs: number = 5000) {
    this.startTime = new Date();
    this.lastUpdate = new Date();
    this.total = total;
    this.updateInterval = updateIntervalMs;
  }

  update(processed: number): void {
    this.processed = processed;
    
    const now = new Date();
    const timeSinceLastUpdate = now.getTime() - this.lastUpdate.getTime();
    
    if (timeSinceLastUpdate >= this.updateInterval) {
      this.logProgress();
      this.lastUpdate = now;
    }
  }

  private logProgress(): void {
    const elapsed = (new Date().getTime() - this.startTime.getTime()) / 1000;
    const rate = this.processed / elapsed;
    
    let message = `Processed: ${this.processed.toLocaleString()} rows`;
    
    if (this.total) {
      const percent = ((this.processed / this.total) * 100).toFixed(1);
      const remaining = this.total - this.processed;
      const eta = remaining / rate;
      
      message += ` (${percent}%)`;
      if (eta > 0 && eta < Infinity) {
        message += `, ETA: ${this.formatDuration(eta)}`;
      }
    }
    
    message += `, Rate: ${Math.round(rate)}/sec`;
    
    console.log(message);
  }

  complete(): void {
    const elapsed = (new Date().getTime() - this.startTime.getTime()) / 1000;
    const rate = this.processed / elapsed;
    
    console.log(`✅ Completed: ${this.processed.toLocaleString()} rows in ${this.formatDuration(elapsed)} (avg ${Math.round(rate)}/sec)`);
  }

  private formatDuration(seconds: number): string {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    
    if (hours > 0) {
      return `${hours}h ${minutes}m ${secs}s`;
    } else if (minutes > 0) {
      return `${minutes}m ${secs}s`;
    } else {
      return `${secs}s`;
    }
  }
}

export class MemoryMonitor {
  private readonly maxMemoryMB: number;
  private lastCheck: Date = new Date();

  constructor(maxMemoryMB: number = 1024) { // 1GB default
    this.maxMemoryMB = maxMemoryMB;
  }

  checkMemoryUsage(): void {
    const now = new Date();
    const timeSinceLastCheck = now.getTime() - this.lastCheck.getTime();
    
    // Check memory every 30 seconds
    if (timeSinceLastCheck >= 30000) {
      const usage = process.memoryUsage();
      const usedMB = Math.round(usage.heapUsed / 1024 / 1024);
      
      if (usedMB > this.maxMemoryMB) {
        console.warn(`⚠️  High memory usage: ${usedMB}MB (limit: ${this.maxMemoryMB}MB)`);
        
        // Force garbage collection if available
        if (global.gc) {
          console.log('🗑️  Running garbage collection...');
          global.gc();
        }
      }
      
      this.lastCheck = now;
    }
  }

  suggestBatchSize(currentBatchSize: number, targetMemoryMB: number = 512): number {
    const usage = process.memoryUsage();
    const usedMB = Math.round(usage.heapUsed / 1024 / 1024);
    
    if (usedMB > targetMemoryMB) {
      // Reduce batch size
      return Math.max(1000, Math.floor(currentBatchSize * 0.8));
    } else if (usedMB < targetMemoryMB * 0.5) {
      // Can increase batch size
      return Math.min(500000, Math.floor(currentBatchSize * 1.2));
    }
    
    return currentBatchSize;
  }
}