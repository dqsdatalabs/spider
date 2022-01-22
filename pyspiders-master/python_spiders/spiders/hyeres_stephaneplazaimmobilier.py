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
    name = 'hyeres_stephaneplazaimmobilier'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Hyeres_Stephaneplazaimmobilier_PySpider_france"
    start_urls = ['https://hyeres.stephaneplazaimmobilier.com/immobilier-acheter?target=rent&agency_id=319&page=1']  # LEVEL 1

    def start_requests(self):
        yield Request(
            url=self.start_urls[0],
            callback=self.jump,
        )

    def jump(self, response):
        token = response.xpath("//meta[@name='csrf-token']/@content").get()
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
            "x-csrf-token": token,
            "x-requested-with": "XMLHttpRequest",
        }

        yield Request(
            "https://hyeres.stephaneplazaimmobilier.com/search/rent?target=rent&agency_id=319&sort=&idagency=166855",
            callback=self.parse,
            headers=headers,
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        data = json.loads(response.body)
        for item in data["results"]:
            follow_url = f"https://hyeres.stephaneplazaimmobilier.com/immobilier-acheter/{item['id']}/{item['slug']}"
            if get_p_type_string(item['name']):
                yield Request(
                    follow_url, 
                    callback=self.populate_item, 
                    meta={
                        "property_type": get_p_type_string(item['title']),
                        "data": item
                    }
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        data = response.meta.get('data')
        item_loader.add_value("title", data["title"])

        item_loader.add_value("external_id", str(data['id']))
        rent = data['price']
        if rent:
            item_loader.add_value("rent", rent.replace(" ","").strip())
        item_loader.add_value("currency", "EUR")
        
        properties = data["properties"]
        item_loader.add_value("address", f"{properties['adresse']} {data['displayLocation']}")
        item_loader.add_value("city", properties["city"])
        item_loader.add_value("zipcode", properties["codePostal"])
        item_loader.add_value("latitude", properties["latitude"])
        item_loader.add_value("longitude", properties["longitude"])
        
        room_count = properties["bedroom"]
        if room_count and room_count !=0:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_value("room_count", properties["room"])

        square_meters = properties["surface"]
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))

        item_loader.add_value("energy_label", properties["consoEner"])
        
        deposit = properties["depotGarantie"]
        if deposit:
            item_loader.add_value("deposit", deposit.replace("€",""))

        utilities = properties["honorEtatLieux"]
        if utilities:
            item_loader.add_value("utilities", str(utilities).replace("€",""))

        item_loader.add_value("floor", str(properties["floor"]))

        elevator = properties["lift"]
        if elevator:
            item_loader.add_value("elevator", True)

        swimming_pool = properties["piscine"]
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        balcony = properties["balcony"]
        if balcony and balcony!=0:
            item_loader.add_value("balcony", True)

        furnished = properties["meuble"]
        if furnished:
            item_loader.add_value("furnished", True)

        item_loader.add_value("description", data["description"])

        images = [x for x in data["thumbnails"]]
        item_loader.add_value("images", images)

        landlord_name = response.xpath("//div[@class='mtitle']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Stéphane Plaza Immobilier")

        landlord_phone = response.xpath("//div[@class='member-contact']/a/@title").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "04 22 54 54 54")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "duplex" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None