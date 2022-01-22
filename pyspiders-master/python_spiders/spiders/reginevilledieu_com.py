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
import math
import re

class MySpider(Spider):
    name = 'reginevilledieu_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Reginevilledieu_PySpider_france_fr"
    start_urls = ["https://www.reginevilledieu.com/louer/"]
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='bientitle']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': 'apartment'})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))

        desc = "".join(response.xpath("//div[@class='bpresentation']//text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.strip())
        
        address = response.xpath("//span[@class='biencommune']//text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
            
        room_count = response.xpath("//i[@class='i-chambre']/following-sibling::text()").get()
        if room_count:
            if "studio" in room_count.lower():
                item_loader.add_value("room_count","1")
            else:
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//i[@class='i-sdb']/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//i[@class='i-surface']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])
        
        rent = response.xpath("//div[@class='price']/b/text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(",",".").replace(" ","")
            item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", "EUR")
        
        external_id = response.xpath("//span[@class='reference']/b/text()").get()
        item_loader.add_value("external_id", external_id)
        
        deposit = response.xpath("//span[contains(.,'garantie')]/b/text()").get()
        if deposit:
            deposit = deposit.split(" ")[0]
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//span[contains(.,'Charge')]/b/text()").get()
        if utilities:
            utilities = utilities.split(" ")[0]
            item_loader.add_value("utilities", utilities) 
        from datetime import datetime
        import dateparser
        available_date=response.xpath("//li[contains(.,'Disponible')]/text()").get()
        if available_date:
            available_date=available_date.split(":")[-1].lower()
            if available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        
        energy_label = response.xpath("//span[@class='dataconso']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        furnished = response.xpath("//li[contains(.,'Meublé') and contains(.,'Oui')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        terrace = response.xpath("//li[contains(.,'Terrasse') and contains(.,'Oui')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//li[contains(.,'Stationnement') and contains(.,'Oui')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'lon')]/text()").get()
        if latitude_longitude:
            latitude_longitude = latitude_longitude.split("=")[-1].split(";")[0].strip()
            try:
                data = json.loads(latitude_longitude)
                item_loader.add_value("latitude", data["lat"])
                item_loader.add_value("longitude", data["lon"])
            except: pass

        images = [x for x in response.xpath("//div[contains(@class,'bslider')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_phone", "02 33 95 19 57")
        item_loader.add_value("landlord_name", "Régine Villedieu Immobilier")
        item_loader.add_value("landlord_email", "contact@reginevilledieu.com")

        yield item_loader.load_item()