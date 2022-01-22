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
    name = 'your_house_nl'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://your-house.nl/woningaanbod/huur/type-appartement?moveunavailablelistingstothebottom=true",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://your-house.nl/woningaanbod/huur/type-woonhuis?moveunavailablelistingstothebottom=true",
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
        for item in response.xpath("//a[@class='img-container']"):
            status = item.xpath("./div/img/@alt").get()
            if status and ("verhuurd" in status.lower() or "onder optie" in status.lower()):
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
        item_loader.add_value("external_source", "Your_House_PySpider_netherlands")
        item_loader.add_xpath("title", "//title/text()")

        external_id = "".join(response.xpath("//tr[td[.='Referentienummer']]/td[2]/text()").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        rent = "".join(response.xpath("//tr[td[.='Huurprijs']]/td[2]/text()").extract())
        if rent:
            price =  rent.split(",")[0].strip().replace(".","")
            item_loader.add_value("rent_string", price)

        deposit = "".join(response.xpath("//tr[td[.='Borg']]/td[2]/text()").extract())
        if deposit:
            dep =  deposit.split(",")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("deposit", int(float(dep)))

        import dateparser
        available_date = response.xpath("//tr[td[contains(.,'Aangeboden ')]]/td[2]/text()").get()
        if available_date:
            available_date = available_date.strip().split(" ")
            available_date = available_date[-3]+" "+available_date[-2]+" "+available_date[-1]
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                
        utilities = "".join(response.xpath("//tr[td[.='Servicekosten']]/td[2]/text()").extract())
        if utilities:
            uti =  utilities.split(",")[0].strip().split("€")[1].strip()
            item_loader.add_value("utilities", int(float(uti)))

        room_count=""
        room = "".join(response.xpath("//tr[td[.='Aantal kamers']]/td[2]/text()").extract())
        if room:
            if "(" in room:
                room_count = room.split("slaapkamer")[0].strip().split(" ")[-1]
            else:
                room_count = room
            item_loader.add_value("room_count", room_count)

        meters = "".join(response.xpath("//tr[td[.='Gebruiksoppervlakte wonen']]/td[2]/text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters.split("m²")[0].strip())

        floor = "".join(response.xpath("//tr[td[.='Aantal bouwlagen']]/td[2]/text()").extract())
        if floor:
            item_loader.add_value("floor", floor)

        bathroom_count = "".join(response.xpath("//tr[td[.='Aantal badkamers']]/td[2]/text()").extract())
        if bathroom_count:
            if "(" in bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split("(")[0].strip())
            else:
                item_loader.add_value("bathroom_count", bathroom_count)

        energy_label = "".join(response.xpath("//tr[td[.='Energielabel']]/td[2]/text()").extract())
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        desc = "".join(response.xpath("//div[@class='description textblock']/div/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        address = "".join(response.xpath("substring-after(//div[@class='addressprice']/h1/text(),': ')").extract())
        if address:
            zipcode = " ".join(address.split(",")[1].strip().split(" ")[:-2])
            city = address.split(",")[1].strip().split(" ")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("city", city.strip())

        images = [x for x in response.xpath("//meta[@property='og:image']/@content").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        furnished = "".join(response.xpath("//tr[td[.='Inrichting']]/td[1]//text() | //tr/td[contains(.,'gemeubileerd')]/text()").extract())
        if furnished:
            item_loader.add_value("furnished", True)

        latitude_longitude = response.xpath("//script[contains(.,'center')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("center: [")[1].split(",")[0]
            longitude = latitude_longitude.split("center: [")[1].split(",")[1].split("]")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            
        phone = " ".join(response.xpath("//div[@class='data']/a/@href[contains(.,'tel')]").getall()).strip()   
        if phone:
            item_loader.add_value("landlord_phone", phone.split(":")[1].strip())

        email = response.xpath("//div[@class='data']/a[contains(@class,'email')]/text()").get()   
        if email:
            item_loader.add_value("landlord_email", email.strip())
            
        item_loader.add_xpath("landlord_name", "//div[@class='data']/div/text()")
   
        yield item_loader.load_item()