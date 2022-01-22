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
    name = 'chiaramonti_com'
    external_source = "Chiaramonti_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it' 
    start_urls = ['https://www.chiaramonti.com/elenco_immobili_f.asp?idm=8087&idcau2=1']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 13)
        
        seen = False
        for item in response.xpath("//a[contains(.,'Vedi Dettagli')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 13 or seen:
            url = f"https://www.chiaramonti.com/elenco_immobili_f.asp?start={page}&ordinaper=D&idcau2=1&inv="
            yield Request(url, callback=self.parse, meta={"page": page+12})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        prop_type = response.xpath("//div[@class='detail-title']/h2/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//title//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("rif:")[1])

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)


        floor = response.xpath("//ul[contains(@class,'list-three-col list-features')]//li[contains(.,'Piano:')]//text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("Piano:")[1])

        address = response.xpath("//div[@class='detail-title']//h2//text()").get()
        if address:
            address="".join(address.split(" ")[4:])
            address="".join(address.split("/")[0])
            item_loader.add_value("address", address)

        city = response.xpath("//div[@class='detail-title']//h2//text()").get()
        if city:
            city="".join(city.split(" ")[5:])
            city="".join(city.split("/")[0])
            item_loader.add_value("city", city)

        description = response.xpath("//div[contains(@class,'detail-title')]//following-sibling::p//text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath("//div[contains(@class,'property-description detail-block')]//span[contains(@class,'item-price')]//text()").get()
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath("//ul[contains(@class,'list-three-col list-features')]//li[contains(.,'Superficie:')]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("Mq. ")[1])

        room_count = response.xpath("//ul[contains(@class,'list-three-col list-features')]//li[contains(.,'Vani')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Vani")[0])
        else:
            room_count = response.xpath("//ul[contains(@class,'list-three-col list-features')]//li[contains(.,'Camere')]//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split("Camere:")[1])


        utilities = response.xpath("//ul[contains(@class,'list-three-col list-features')]//li[contains(.,'mensili')]//text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("mensili: ")[1].split("€")[0])

        energy_label = response.xpath("//ul[contains(@class,'list-unstyled')]//li//strong[contains(.,'Classe Energetica: ')]//following-sibling::text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        balcony = response.xpath("//ul[contains(@class,'list-three-col list-features')]//li[contains(.,'Balcone')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)

        parking = response.xpath("//ul[contains(@class,'list-three-col list-features')]//li[contains(.,'posto auto')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)

        terrace = response.xpath("//ul[contains(@class,'list-three-col list-features')]//li[contains(.,'Terrazzo')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        else:
            item_loader.add_value("terrace", False)

        latitude_longitude = response.xpath(
            "//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                'google.maps.LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split(
                'google.maps.LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'slide')]//div//a[contains(@class,'overlay')]//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "CHIARAMONTI IMMOBILIARE")
        item_loader.add_value("landlord_phone", "055332014")
        item_loader.add_value("landlord_email", "barbara@chiaramonti.com")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("bilocale" in p_type_string.lower() or "trilocale" in p_type_string.lower() or "quadrilocale" in p_type_string.lower()):
        return "house"
    elif p_type_string and "stanza" in p_type_string.lower():
        return "room"
    else:
        return None