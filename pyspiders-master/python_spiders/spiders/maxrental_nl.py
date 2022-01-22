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

class MySpider(Spider):
    name = 'maxrental_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = 'Maxrental_PySpider_netherlands_nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.maxrentalhuurwoning.nl/woningaanbod/huur/type-appartement?iscustom=true",
                "property_type" : "apartment",
            },
            {
                "url" : "https://www.maxrentalhuurwoning.nl/woningaanbod/huur/type-woonhuis?iscustom=true",
                "property_type" : "house",
            },
        ]
        
        for item in start_urls: yield Request(item["url"], callback=self.parse, meta={'property_type': item['property_type']})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'object_list col col-md-12')]//div[@class='sys-image-container']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[contains(@class,'next-page')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Maxrental_PySpider_" + self.country + "_" + self.locale)
        rented = response.xpath("//div[contains(@class,'rented_under_conditions') or contains(@class,'rented')]/text()[contains(.,'Verhuurd')]").get()
        if rented:
            return

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url.split("?")[0])

        prop = response.xpath("//div[@class='table-responsive feautures']//tr[./td[.='Type object']]/td[2]/text()").get()
        if prop:
            prop = prop.split(",")[0].strip()
            if "Appartement" in prop:
                prop = "apartment"
            elif "Woonhuis" in prop:
                prop = "house"
            item_loader.add_value("property_type", prop)

        desc = "".join(response.xpath("//div[@class='description']/text()").extract())
        item_loader.add_value("description", desc.strip())

        price = response.xpath("//div[@class='table-responsive feautures']//tr[./td[.='Huurprijs']]/td[2]/text()").get()
        if price:
            item_loader.add_value(
                "rent", price.split("€")[1].split(",")[0])
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//div[@class='table-responsive feautures']//tr[./td[.='Borg']]/td[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[1].split(",")[0])

        item_loader.add_xpath(
            "external_id", "//div[@class='table-responsive feautures']//tr[./td[.='Referentienummer']]/td[2]/text()"
        )

        square = response.xpath(
            "//div[@class='table-responsive feautures']//tr[./td[.='Gebruiksoppervlakte wonen']]/td[2]/text()"
        ).get()
        if square:
            item_loader.add_value(
                "square_meters", square.split("m²")[0]
            )
        
        room_count = response.xpath(
            "//div[@class='table-responsive feautures']//tr[./td[.='Aantal kamers']]/td[2]/text()"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("slaapkamer")[0].strip().split(" ")[-1])

        bathroom_count = response.xpath("//div[@class='table-responsive feautures']//tr[./td[.='Aantal badkamers']]/td[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])

        address = response.xpath("normalize-space(//span[@class='obj_sub_address']/text())").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", split_address(address, "city"))
        else:
            address = response.xpath("//h1[@class='obj_address']/text()").get()
            if address:
                address = address.split(": ")[1]
                item_loader.add_value("address", address)
                item_loader.add_value("zipcode", split_address(address, "zip"))
                item_loader.add_value("city", split_address(address, "city"))
            
        available_date = response.xpath(
            "//div[@class='table-responsive feautures']//tr[./td[.='Aanvaarding']]/td[2]/text()[.!='Direct' and .!='In overleg']").get()
        if not available_date:
            available_date = response.xpath(
            "//div[@class='table-responsive feautures']//tr[./td[.='Aangeboden sinds']]/td[2]/text()[.!='Direct' and .!='In overleg']").get()
        if available_date:
            y = available_date.split(" ")[-1]
            m = available_date.split(" ")[-2]
            d = available_date.split(" ")[-3]
            date_splitted = d + " " + m + " " + y
            date_parsed = dateparser.parse(
                date_splitted, date_formats=["%d %B %Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        

        floor = response.xpath("//div[@class='table-responsive feautures']//tr[./td[.='Woonlaag']]/td[2]/text()").get()
        if floor:
            item_loader.add_value(
                "floor", floor.split("e")[0]
            )


        furnished = response.xpath(
            "//div[@class='table-responsive feautures']//tr[./td[.='Inrichting']][1]/td[2]/text()"
        ).get()
        if furnished:
            if "Ja" in furnished:
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        

        elevator = response.xpath(
            "//div[@class='table-responsive feautures']//tr[./td[.='Heeft een lift']]/td[2]/text()").get()
        if elevator:
            if "Ja" in elevator:
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        
        balcony = response.xpath(
            "//div[@class='table-responsive feautures']//tr[./td[.='Heeft een balkon']]/td[2]/text()").get()
        if balcony:
            if "Ja" in balcony:
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)

        parking = response.xpath(
            "//div[@class='table-responsive feautures']//tr[./td[.='Heeft een garage']]/td[2]/text()").get()
        if parking:
            if "Ja" in parking:
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@id='object-photos']//div[@class='small item']/img/@src | .//div[@class='gallery']/a/img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", list(set(images)))
        
        
        item_loader.add_value("landlord_phone", "010 7640958")
        item_loader.add_value("landlord_name", "Max Property Group")
        item_loader.add_value("landlord_email", "rentals@maxpropertygroup.com")

        yield item_loader.load_item()

def split_address(address, get):

    if "," in address:
        temp = address.split(",")[1]
        zip_code = "".join(filter(lambda i: i.isdigit(),temp))
        city = address.split(" ")[-1]

        if get == "zip":
            return zip_code
        else:
            return city