# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'victoire_be'
    execution_type='testing'
    country='belgium'
    locale='fr'
    custom_settings = {
        "PROXY_ON": True
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://victoire.be/fr/a-louer?term=&type%5B%5D=appartement&minPrice=&maxPrice=&bedrooms=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://victoire.be/fr/a-louer?term=&type%5B%5D=maison&minPrice=&maxPrice=&bedrooms="
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//a[contains(@id,'estate')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"https://victoire.be/fr/a-louer/all/{page}?{response.url.split('?')[1]}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Victoire_PySpider_belgium")
        
        item_loader.add_value("external_id", response.url.split("detail/")[1].split("/")[0])
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        city = response.xpath("//span[@class='city']/text()").get()
        if city:
            item_loader.add_value("address", city)
            item_loader.add_value("city", city)
        
        rent = response.xpath("//div/div[@class='specs']/span[contains(.,'€')]/text()").get()
        if rent:
            rent = rent.replace(".","").split("€")[1].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//div[p[contains(.,'habitable')]]/p[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])
        
        room_count = response.xpath("//div[p[contains(.,'Chambre')]]/p[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div[p[contains(.,'Salle')]]/p[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        import dateparser
        available_date = response.xpath("//div[@class='location']/span/text()[contains(.,'Disponible')]").get()
        if available_date:
            available_date= available_date.split(":")[1].strip()
            if "immédiatement" not in available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        desc = " ".join(response.xpath("//div[contains(@class,'description')]/p//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        images = [x for x in response.xpath("//div[@class='slider']//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        parking = response.xpath("//div[span[contains(.,'Parking')]]/span[2]/text()").get()
        if parking and parking.strip() !='0':
            item_loader.add_value("parking", True)
        
        energy_label = response.xpath("//div[span[contains(.,'EPC categorie')]]/span[2]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
            
        floor = response.xpath("//div[span[contains(.,'Étage')]]/span[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        furnished=response.xpath("//p[@class='result']/text()[.='Oui']").get()
        if furnished and "oui" in furnished.lower():
            item_loader.add_value("furnished",True)
            
        
        deposit = response.xpath("//div[span[contains(.,'Dépôt')]]/span[2]/text()").get()
        if deposit:
            deposit = deposit.strip().split(" ")[0]
            item_loader.add_value("deposit", int(float(rent))*int(deposit))
        
        elevator = response.xpath("//div[span[contains(.,'Ascenseur')]]//@alt[contains(.,'check')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//div[span[contains(.,'Balcon')]]//@alt[contains(.,'check')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[p[contains(.,'terrasse')]]/p[2]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        item_loader.add_value("landlord_name", "Victoire")
        item_loader.add_value("landlord_phone", "+32 2 375 10 10")
        item_loader.add_value("landlord_email", "info@victoire.be")

        yield item_loader.load_item()