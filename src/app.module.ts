import { Module } from '@nestjs/common';
import { ConfigService } from './config.service';
import { LogCollectorService } from './collector.service';

@Module({
  imports: [],
  controllers: [],
  providers: [ConfigService, LogCollectorService],
})
export class AppModule { }
