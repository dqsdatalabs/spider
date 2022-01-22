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
import dateparser

class MySpider(Spider):
    name = 'regio56makelaars_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale ='nl'
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.regio56makelaars.nl/woningaanbod/huur/type-appartement",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.regio56makelaars.nl/woningaanbod/huur/type-woonhuis"
                ],
                "property_type": "house"
            }
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
        
        for item in response.xpath("//article[contains(@class,'objectcontainer')]"):
            follow_url = response.urljoin(item.xpath(".//a[contains(@class,'sys-property-link')]//@href").get())
            status = item.xpath(".//span[contains(@class,'rented')]/text()").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

        page = response.xpath("//a[contains(@class,'next-page')]//@href").get()
        if page:
            url = response.urljoin(page)
            yield Request(url, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Regio56makelaars_PySpider_netherlands")
        
        title = response.xpath("//h2[@class='object_title']/text()").get()
        if title:
            item_loader.add_value("title", title)
            
            address = title.split(":")[1].strip()
            item_loader.add_value("address", address)
            city = title.split(" ")[-1]
            item_loader.add_value("city", city) 
            zipcode = title.split(",")[1].split(city)[0].strip()
            item_loader.add_value("zipcode", zipcode)
        if title and "verhuurd" in title.lower():
            return 
        external_id = response.xpath("//tr[td[contains(.,'Referentienummer')]]/td[2]/text()").get()
        item_loader.add_value("external_id", external_id)
        
        square_meters = response.xpath("//tr[td[contains(.,'Gebruiksoppervlakte wonen')]]/td[2]/text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//tr[td[contains(.,'Aantal kamers')]]/td[2]/text()").get()
        if room_count:
            if "(" in room_count: room_count = room_count.split("(")[0].strip()
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//tr[td[contains(.,'badkamers')]]/td[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//tr[td[contains(.,'Huurprijs') or contains(.,'Prijs')]]/td[2]/text()[contains(.,'€')]").get()
        if rent:
            rent = rent.split(",-")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//tr[td[contains(.,'Borg')]]/td[2]/text()").get()
        if deposit:
            deposit = deposit.split(",-")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//tr[td[contains(.,'Servicekosten')]]/td[2]/text()").get()
        if utilities:
            utilities = utilities.split(",-")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("utilities", utilities)
        
        furnished = response.xpath("//tr[td[contains(.,'Inrichting')]]/td[2]/text()[contains(.,'Gemeubileerd')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        floor = response.xpath("//tr[td[contains(.,'Woonlaag')]]/td[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0])
        
        available_date = response.xpath("//tr[td[contains(.,'Aanvaarding')]]/td[2]/text()").get()
        if available_date and "direct" not in available_date.lower():
            try:
                available_date = f"{available_date.split(' ')[-3]} {available_date.split(' ')[-2]} {available_date.split(' ')[-1]}"
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            except: pass
        
        desc="".join(response.xpath("//div[contains(@class,'description')]//div//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        images = [x for x in response.xpath("//div[@id='object-photos']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        lat_lng = response.xpath("//script[contains(.,'center: [')]/text()").get()
        if lat_lng:
            latitude = lat_lng.split("center: [")[1].split(",")[0].strip()
            longitude = lat_lng.split("center: [")[1].split(",")[1].split("]")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        landlord_name = response.xpath("//div[contains(@class,'contact_name')]/text()").get()
        item_loader.add_value("landlord_name", landlord_name)
            
        landlord_phone = response.xpath("//div[contains(@class,'contact_phone')]/text() | //div[@class='object_detail_department_phone']/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        
        landlord_email = response.xpath("//a[contains(@class,'contact_email')]/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.strip())
        
        yield item_loader.load_item()