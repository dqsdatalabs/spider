# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'mauritshuis_info'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.mauritshuis.info/woningaanbod/huur/type-appartement",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.mauritshuis.info/woningaanbod/huur/type-woonhuis",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'object')]/a"):
            status = item.xpath("./div[@class='content']/img/@alt").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//a[contains(@class,'next-page')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Mauritshuis_PySpider_netherlands")

        external_id = response.xpath("//td[contains(.,'Referentienummer')]/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.split(':')[-1].strip())
            item_loader.add_value("zipcode", address.split(':')[-1].split(',')[-1].strip().split(' ')[0].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='description textblock']/div//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//td[contains(.,'Gebruiksoppervlakte wonen')]/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].strip())

        room_count = response.xpath("//td[contains(.,'Aantal kamers')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split('slaapkamer')[0].strip().split(' ')[-1].strip())

        deposit = response.xpath("//td[contains(.,'Borg')]/following-sibling::td/text()").get()
        if deposit:
            dep = deposit.split(",")[0].replace(".","").strip()
            item_loader.add_value("deposit", dep)
        
        bathroom_count = response.xpath("//td[contains(.,'Aantal badkamers')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//div[@class='object_price']/text()").get()
        if rent:
            rent = rent.split('€')[-1].split(',')[0].strip().replace('.', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//td[contains(.,'Aanvaarding') or contains(.,'Beschikbaar vanaf')]/following-sibling::td/text()").get()
        if available_date:
            available_date = available_date.replace("Per", "").replace("zaterdag","").replace("maandag","").replace("donderdag","").replace("woensdag","").strip()
            date_parsed = dateparser.parse(available_date.strip())
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='object-photos']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='object-floorplans']/a/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'lat:')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('lat:')[-1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('lon:')[-1].split(',')[0].strip())
        
        energy_label = response.xpath("//td[contains(.,'Energielabel')]/following-sibling::td/text()").get()
        if energy_label:
            if energy_label.strip().upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.strip().upper())
        
        floor = response.xpath("//td[contains(.,'Woonlaag')]/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", "".join(filter(str.isnumeric, floor.strip())))

        utilities = response.xpath("//td[contains(.,'Servicekosten')]/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[-1].split(',')[0].strip().replace('.', ''))

        parking = response.xpath("//td[contains(.,'Heeft een garage')]/following-sibling::td/text()").get()
        if parking:
            if parking.strip().lower() == 'ja':
                item_loader.add_value("parking", True)
            elif parking.strip().lower() == 'nee':
                item_loader.add_value("parking", False)

        balcony = response.xpath("//td[contains(.,'Heeft een balkon')]/following-sibling::td/text()").get()
        if balcony:
            if balcony.strip().lower() == 'ja':
                item_loader.add_value("balcony", True)
            elif balcony.strip().lower() == 'nee':
                item_loader.add_value("balcony", False)

        furnished = " ".join(response.xpath("//td[contains(.,'Inrichting')]/following-sibling::td/text()").getall()).strip()
        if furnished:
            if 'niet' in furnished.strip().lower():
                item_loader.add_value("furnished", False)
            elif 'gestoffeerd' in furnished.strip().lower() or 'gemeubileerd' in furnished.strip().lower():
                item_loader.add_value("furnished", True)

        elevator = response.xpath("//td[contains(.,'Heeft een lift')]/following-sibling::td/text()").get()
        if elevator:
            if elevator.strip().lower() == 'ja':
                item_loader.add_value("elevator", True)
            elif elevator.strip().lower() == 'nee':
                item_loader.add_value("elevator", False)

        item_loader.add_value("landlord_name", "Mauritshuis Makelaars")
        item_loader.add_value("landlord_phone", "+31 (0) 26 44 64 300")
        item_loader.add_value("landlord_email", "wonen@mauritshuis.info")
      
        yield item_loader.load_item()