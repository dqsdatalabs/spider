# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'rentanapartment_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'

    def start_requests(self):

        start_urls = [
            {"url": "https://www.rentanapartment.nl/woningaanbod/huur/type-appartement", "property_type": "apartment"},
            {"url": "https://www.rentanapartment.nl/woningaanbod/huur/type-woonhuis", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "base_url":url.get('url')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'col-md-12 object_list')]//div[contains(@class,'listgal')]//div[contains(@class,'object_data_col')]/a/@href").extract():
            follow_url = response.urljoin(item).split("?")[0]
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        pagination = response.xpath("//div[contains(@class,'pagination-top')]/ul/li/a[@class='sys_paging next-page']/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        status = "".join(response.xpath("//div[contains(@class,'object_status')]//text()").getall())
        if status and "verhuurd" in status.lower():
            return
        item_loader.add_value("external_source", "Rentanapartment_PySpider_" + self.country + "_" + self.locale)

        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            
        item_loader.add_value("external_link", response.url)
        external_id = response.xpath("//td[.='Referentienummer']/parent::tr/td[2]/text()").get() 
        item_loader.add_value("external_id", external_id)

        desc="".join(response.xpath("//h3[.='Beschrijving']/parent::div/div/text() | //h3[.='Indeling']/parent::div/div/text() | //h3[.='Bijzonderheden']/parent::div/div/text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description",desc)
            if "GEMEUBILEERD" in desc.upper():
                item_loader.add_value("furnished", True)
            if "huisdieren niet toegestaan" in desc:
                item_loader.add_value("pets_allowed", False)
        
        latitude_longitude = response.xpath("//script[contains(.,'object_detail_map')]/text()").get()
        if latitude_longitude:
            latitude_longitude = latitude_longitude.split('[')[1].split(']')[0]
            longitude = latitude_longitude.split(',')[0].strip()
            latitude = latitude_longitude.split(',')[1].strip()          
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        address = response.xpath("//h1/text()").get()
        if address:
            address = address.split(":")[1].strip()
            item_loader.add_value("address", address)
            # city = address.split(",")[1].strip().split(" ")[-1]
            # item_loader.add_value("zipcode", address.split(",")[1].split(city)[0].strip())
            # item_loader.add_value("city", city)
            city_zipcode = address.split(",")[1].strip()
            item_loader.add_value("zipcode", split_address(city_zipcode, "zip"))
            item_loader.add_value("city", split_address(city_zipcode, "city").strip())
        floor = response.xpath("//td[.='Aantal bouwlagen']/parent::tr/td[2]/text()").get()
        if floor:
            floor = floor.strip()
        item_loader.add_value("floor", floor)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        square_meters = response.xpath("//td[.='Gebruiksoppervlakte wonen']/parent::tr/td[2]/text()").get()
        if square_meters:
            square_meters = square_meters.split(' ')[0]
        item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//td[.='Aantal kamers']/parent::tr/td[2]/text()").get()
        if room_count:
            if "slaapkamer" in room_count.lower():
                room_count = room_count.lower().split("slaapkamer")[0].strip().split(" ")[-1].strip()
            else:
                room_count = room_count.split(' ')[0].strip()
            
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//td[.='Aantal badkamers']/parent::tr/td[2]/text()").get()
        if bathroom_count:
            if len(bathroom_count.split(' ')) > 1:
                bathroom_count = bathroom_count.split(' ')[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        elif not bathroom_count:
            if desc:
                if "badkamer" in desc.lower():
                    item_loader.add_value("bathroom_count", "1")
        
        price = response.xpath("//div[@class='block price clearfix']/span/text()").get()
        if price: 
            price = price.split(',')[0].split(' ')[1]
        item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        available_date = response.xpath("//h3[.='Beschrijving']/parent::div/div/text()[contains(.,'Beschikbaar per ')] | //h3[.='Indeling']/parent::div/div/text()[contains(.,'Beschikbaar per ')] | //h3[.='Bijzonderheden']/parent::div/div/text()[contains(.,'Beschikbaar per ')]").get()
        if available_date:
            try:
                date_parsed = dateparser.parse(available_date.split("per")[1].strip(), date_formats=["%d/%m/%Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            except:
                pass

        images = [x for x in response.xpath("//h3[contains(.,'Foto')]/parent::div/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        deposit = response.xpath("//td[@class='object_detail_title' and .='Borg']/parent::tr/td[2]/text()").get()
        if deposit:
            if len(deposit.split(',')) > 1:
                deposit = deposit.split(',')[0].split(' ')[1]
            else:
                deposit = deposit.split(' ')[1]
            item_loader.add_value("deposit", deposit)
       
        utilities = response.xpath("//td[@class='object_detail_title' and .='Servicekosten']/parent::tr/td[2]/text()").get()
        if utilities:
            if len(utilities.split(',')) > 1:
                utilities = utilities.split(',')[0].split(' ')[1]
            else:
                utilities = utilities.split(' ')[1]
            item_loader.add_value("utilities", utilities)

        furnished = response.xpath("//td[.='Inrichting']/parent::tr/td[2]/text()").get()
        if furnished:
            if furnished.lower() == "ja":
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        balcony = response.xpath("//td[contains(.,'balkon')]/parent::tr/td[2]/text()").get()
        if balcony:
            if balcony.lower() == "ja":
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)

        item_loader.add_value("landlord_name", "Rent an Apartment")
        item_loader.add_value("landlord_email", "info@rentanapartment.nl")
        item_loader.add_value("landlord_phone", "+ 31 10 302 80 44")
        yield item_loader.load_item()
def split_address(address, get):
    temp = address.split(" ")[0]+" "+address.split(" ")[1]
    zip_code = temp
    city = address.split(temp)[1]

    if get == "zip":
        return zip_code
    else:
        return city