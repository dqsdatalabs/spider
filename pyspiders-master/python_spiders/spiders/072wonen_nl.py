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

class MySpider(Spider):
    name = '072wonen_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.072wonen.nl/woningaanbod/huur/type-appartement",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.072wonen.nl/woningaanbod/huur/type-woonhuis",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//article[contains(@class,'object')]"):
            status = item.xpath(".//span[contains(@class,'object_status')]/text()").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath(".//a//@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[contains(@class,'next-page')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )    
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "072wonen_PySpider_netherlands")      
        item_loader.add_xpath("title", "//title/text()")

        external_id = "".join(response.xpath("//tr[td[.='Referentienummer']]/td[2]/text()").getall())
        if external_id:
            item_loader.add_value("external_id",external_id.strip())

        desc = "".join(response.xpath("//div[contains(@class,'description')]/div/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        LatLng = "".join(response.xpath("substring-before(substring-after(//script/text()[contains(.,'center')],'center: ['),',')").getall())
        if LatLng:
            item_loader.add_value("latitude",LatLng.strip())
            item_loader.add_xpath("longitude","substring-before(substring-after(substring-after(//script/text()[contains(.,'center')],'center: ['),', '),']')")


        images = [x for x in response.xpath("//div[@class='sys-object-picture-container']/a/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        address = "".join(response.xpath("substring-after(//h1[@class='obj_address']/text(),': ')").getall())
        if address:
            zipcode = address.split(",")[1].strip().split(" ")[0]
            city = address.split(",")[1].strip().split(zipcode)[1].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())

        rent = "".join(response.xpath("//span[contains(@class,'object_price')]/text()[contains(.,'€')]").getall())
        if rent:
            price = rent.split(",")[0].strip().replace(",","").replace(".","")
            item_loader.add_value("rent_string",price.strip())

        deposit = "".join(response.xpath("//tr[td[.='Borg']]/td[2]/text()").getall())
        if deposit:
            dep = deposit.split(",")[0].strip().replace(",","").replace(".","")
            item_loader.add_value("deposit",dep.strip())

        energy_label = "".join(response.xpath("//tr[td[.='Energielabel']]/td[2]/text()").getall())
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip())

        utilities = "".join(response.xpath("//tr[td[.='Servicekosten']]/td[2]/text()").getall())
        if utilities:
            uti = utilities.split(",")[0].strip().replace(",","").replace(".","")
            item_loader.add_value("utilities",uti.strip())

        meters = "".join(response.xpath("//tr[td[.='Gebruiksoppervlakte wonen']]/td[2]/text()").getall())
        if meters:
            s_meters = meters.split("m²")[0].replace(",",".").strip()
            item_loader.add_value("square_meters",int(float(s_meters)))

        date2 = ""
        available_date=response.xpath("//tr[td[.='Beschikbaar vanaf']]/td[2]/text()[.!='In overleg'] | //tr[td[.='Aanvaarding']]/td[2]/text()[.!='In overleg']").get()
        if available_date:
            if "maandag" in available_date.lower():
                date2 = available_date.lower().split("maandag")[1].strip()
            elif "donderdag" in available_date.lower():
                date2 = available_date.lower().split("donderdag")[1].strip()
            elif 'zondag' in available_date.lower(): date2 = available_date.lower().split("zondag")[1].strip()
            elif 'vrijdag' in available_date.lower(): date2 = available_date.lower().split("vrijdag")[1].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"], languages=["nl"]
            )
            if date_parsed:
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)


        room_count = "".join(response.xpath("//tr[td[.='Aantal bouwlagen']]/td[2]/text()").getall())
        if room_count:
            room = room_count.strip()
            if room !="0":
                item_loader.add_value("room_count",room.strip())

        floor = "".join(response.xpath("//tr[td[.='Woonlaag']]/td[2]/text()").getall())
        if floor:
            item_loader.add_value("floor",floor.strip())

        bathroom = ""
        bathroom_count = "".join(response.xpath("//tr[td[.='Aantal badkamers']]/td[2]/text()").getall())
        if bathroom_count:
            if "(" in bathroom_count:
                bathroom = bathroom_count.split("(")[0]
            else:
                bathroom = bathroom_count
            item_loader.add_value("bathroom_count",bathroom.strip())

        furnished = "".join(response.xpath("//tr[td[.='Inrichting']]/td[2]/text()").getall())
        if furnished:
            if "ja" in furnished.lower() :
                item_loader.add_value("furnished",True)

        washing_machine = "".join(response.xpath("//tr[td[.='Badkamervoorzieningen']]/td[2]/text()[contains(.,'Wasmachineaansluiting')]").getall())
        if washing_machine:
            item_loader.add_value("washing_machine",True)

        item_loader.add_value("landlord_phone", "072-5208259")
        item_loader.add_value("landlord_name", "072wonen")
        item_loader.add_value("landlord_email", "info@072wonen.nl")

              
        yield item_loader.load_item()