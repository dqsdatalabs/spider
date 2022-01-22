# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re
from datetime import datetime


class MySpider(Spider):
    name = 'amsterhomes_com'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    external_source='Amsterhomes_PySpider_netherlands'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.amsterhomes.com/woningen", "property_type": "apartment"},            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for item in response.xpath("//a[@class='thumbnail']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("(//div[@class='ah-style-esn']/dl/dt[contains(.,'Status')]/following-sibling::dd//text())[1]").get()
        if status and "Ingetrokken" in status:
            return
        if status and "Verhuurd" in status:
            return
        item_loader.add_value("external_source", "Amsterhomes_PySpider_netherlands")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = response.xpath("//h1[@class='mh-top-title__heading']//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('?p=')[-1])
       
        item_loader.add_value("city", "Amsterdam")
        address = response.xpath("//h1[@class='mh-top-title__heading']//text()").get()
        if address:
            item_loader.add_value("address", address)
        rent = response.xpath("//dl/dt[contains(.,'Huurprijs')]//following-sibling::dd[1]//text()").get()
        price = ""
        if rent:
            price = rent.split(',')[0].split('€')[1].strip()
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("(//dl/dt[contains(.,'Gebruiks ')]//following-sibling::dd[1]//text())[1]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        room_count = response.xpath("//dl/dt[contains(.,'Aantal slaapkamers')]//following-sibling::dd[1]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        # bathroom_count = response.xpath("//p[contains(@class,'desc')]//text()[contains(.,'badkamers')]").get()
        # if bathroom_count:
        #     bathroom_count = bathroom_count.split("badkamers")[0].strip().split(" ")[-1]
        #     if "twee" in bathroom_count.lower():
        #         item_loader.add_value("bathroom_count", "2")
        #     elif bathroom_count.isdigit():
        #         item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = "".join(response.xpath("//div[@class='ah-bg-style']//p//text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            if desc and "furnished" in desc.lower():
                item_loader.add_value("furnished", True)
            if desc and "parking" in desc.lower():
                item_loader.add_value("parking", True)
            if desc and "bathroom" in desc.lower():
                bathroom = desc.split('bathrooms')[0].strip().split('–')[-1].strip().split(',')[-1].strip().split(' ')[-1].strip()
                if bathroom.isdigit():
                    item_loader.add_value("bathroom_count", bathroom)
                else:
                    item_loader.add_value("bathroom_count", "1")
            if desc and "available" in desc.lower():
                available_date = desc.split('Available per')[-1].strip().split('–')[0].split(',')[0].strip().replace("st","").replace("nd","").replace("rd","").replace("th","").strip()
                if available_date:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
                else:
                    available_date = desc.split('beschikbaar per')[-1].strip().split('–')[0].split('Omgeving')[0].strip().split('voor')[0].strip()
                    if available_date:
                        date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        images = [ x for x in response.xpath("//div[@id='content']//a//@href").getall()]
        if images:
            item_loader.add_value("images", images)        
        
        available_date = response.xpath("//ul/li[contains(.,'Beschikbaar')]/span[contains(@class,'desc')]/text()").get()
        if available_date:
            if "Direct" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        floor_plan_images = [x for x in response.xpath("//h2/following-sibling::img//@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
              
        furnished = response.xpath(
            "//ul/li[contains(.,'Interieur')]/span[contains(@class,'desc')]/text()[contains(.,'Gemeubileerd') or contains(.,'Gestoffeerd')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        pets_allowed = response.xpath("//p[contains(@class,'desc')]//text()[contains(.,'no pet')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)
        
        terrace = response.xpath("//p[contains(@class,'desc')]//text()[contains(.,'terrace') or contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//p[contains(@class,'desc')]//text()[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//p[contains(@class,'desc')]//text()[contains(.,'balkon') or contains(.,'Balkon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        pets_allowed = response.xpath("//p[contains(@class,'desc')]//text()[contains(.,'- geen huisdieren toegestaan')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)
        
        item_loader.add_value("landlord_name", "AmsterHomes")
        item_loader.add_value("landlord_phone", "31 020 820 44 21")
        item_loader.add_value("landlord_email", "info@amsterhomes.com")
        
        yield item_loader.load_item()