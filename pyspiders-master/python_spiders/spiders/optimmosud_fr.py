# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser

class MySpider(Spider):
    name = 'optimmosud_fr' 
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source="Optimmosud_PySpider_france"

    # 1. FOLLOWING
    def start_requests(self):
        start_urls = [
            {"url": "http://www.optimmo-sud.fr/en/search/", "property_type": "apartment", "type":"1"},
            {"url": "http://www.optimmo-sud.fr/en/search/", "property_type": "house", "type":"2"},
        ]  # LEVEL 1
        
        for url in start_urls:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
            }

            data = {
                "nature": "2",
                "type[]": url.get("type"),
                "price": "",
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "newprogram_delivery_at": "",
                "newprogram_delivery_at_display": "",
                "currency": "EUR",
                "customroute": "",
                "homepage": "",
            }

            yield FormRequest(
                url=url.get("url"),
                formdata=data,
                headers=headers,
                callback=self.parse,
                meta={'property_type': url.get('property_type'),
                        "type":url.get('type')}
            )
 
    def parse(self, response):
        for url in response.xpath("//span[contains(@class,'selectionLink ')]"):
            base_url = "http://www.optimmo-sud.fr/en/popup/"
            follow_url = url.xpath("./@href").get()
            follow_url = follow_url.split("=")[1]
            follow_url = base_url + follow_url

            yield Request(follow_url,callback=self.jump, meta={'property_type': response.meta.get('property_type')})

    def jump(self, response):
        base_url = "http://www.optimmo-sud.fr"
        detail_url = response.xpath("//a[@class='button']/@href").get()
        detail_url = base_url + detail_url
        yield Request(detail_url,callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
  
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        title = response.xpath("//div[@class='titles']/h1/text()").get()
        if title:
            item_loader.add_value("title", title)

        rent = response.xpath("//span[@class='selectionLink ']/parent::div/ul/li[contains(.,'€')]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace(",","").replace(",",""))
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath("//li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(' ')[0]
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//li[contains(.,'Rooms')]/span/text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)      
        
        bathroom = response.xpath("//div//li[contains(.,'bathroom')]//text()").extract_first()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split("bathroom")[0])

        deposit = response.xpath("//li[contains(.,'Guarantee')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("€","").replace(",","").replace(".","").strip())

        city = response.xpath("//span[@class='selectionLink ']/../h2/br/following-sibling::text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        
        address=response.xpath("normalize-space(//p[contains(@class,'address')]/text())").get()
        if address:
            item_loader.add_value("address", address)
        
        external_id = response.xpath("//span[@class='selectionLink ']/parent::div/ul/li[1]/text()").get()
        if external_id:
            external_id = external_id.split('.')[1].strip()
        item_loader.add_value("external_id", external_id)

        images = [x for x in response.xpath("//div[@class='show-carousel owl-carousel owl-theme']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        furnished = response.xpath("//div[@class='services details']/ul/li[.='Furnished']").get()
        if furnished:
            furnished = True
        item_loader.add_value("furnished", furnished)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]").get()
        if latitude_longitude:
            latitude_longitude = latitude_longitude.split('L.marker([')[1].split(']')[0]
            latitude = latitude_longitude.split(',')[0].strip()
            longitude = latitude_longitude.split(',')[1].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        elevator = response.xpath("//li[.='Lift']/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        washing_machine = response.xpath("//li[.='Washing machine']/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        floor = response.xpath("//li[contains(.,'Floor')]/span/text()").get()
        if floor:
            floor = floor.split('/')[0].strip().lower()
        item_loader.add_value("floor", floor)

        terrace = response.xpath("//div[@class='areas details']/ul/li[contains(.,'Terrace')]/text()").get()
        if terrace:
            terrace = True
        item_loader.add_value("terrace", terrace)

        swimming_pool = response.xpath("//div[@class='services details']/ul/li[contains(.,'Swimming pool')]/text()").get()
        if swimming_pool:
            swimming_pool = True
        item_loader.add_value("swimming_pool", swimming_pool)

        utilities = response.xpath("//div//li[contains(.,'Inventory')]/span//text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0])

        energy = response.xpath("//div[h2[contains(.,'Energy')]]/img[1]/@src").extract_first()
        if energy:
            energy_label = energy.split("/")[-1]
            if "%" in energy_label:
                energy_label = energy_label.split("%")[0]
            item_loader.add_value("energy_label",energy_label_calculate(energy_label.strip()))
        
        available_date = response.xpath("//div//li[contains(.,'Available at ')]/span//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        item_loader.add_value("landlord_name", "OPTIMMO SUD - LABASTIDE IMMOBILIER")
        item_loader.add_value("landlord_phone", "+33 5 61 83 63 90")

        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label
