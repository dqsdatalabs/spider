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
    name = "maribrik"
    allowed_domains = ["maribrik.be"]
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    external_source='Maribrik_PySpider_belgium_nl'

    def start_requests(self):
        start_urls = [
            {"url": "http://www.maribrik.be/te-huur/?type=12&city=&rooms=0&startp=0&endp=2500&sort=sort", "property_type": "apartment"},
	        {"url": "http://www.maribrik.be/te-huur/?type=26&city=&rooms=0&startp=0&endp=2500&sort=sort", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    def parse(self, response, **kwargs):
        for item in response.xpath("//a[@class='property-item']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Maribrik_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        rent = response.xpath("//h3[contains(@class,'price')]/text()").get()
        if rent:
            price = rent.strip().split(" ")[-1]
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        address = " ".join(response.xpath(
            "//div/h3[contains(.,'Over dit')]/parent::div/ul//li[1]//text() | //div/h3[contains(.,'Over dit')]/parent::div/ul//li[2]//text()").getall())
        if address:
            item_loader.add_value("address", address)
        
        city_zipcode = response.xpath("//div/h3[contains(.,'Over dit')]/parent::div/ul/li[2]//text()").get()
        if city_zipcode:
            city_zipcode = city_zipcode.split(" ")
            if city_zipcode[0].isdigit():
                item_loader.add_value("zipcode", city_zipcode[0])
                item_loader.add_value("city", city_zipcode[1])
        
        room_count = response.xpath("//tr/td[contains(.,'Slaapkamer')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        square_meters = response.xpath("//ul/li/strong[contains(.,'Woonopp')]/parent::li/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip().split("m")[0])

        bathroom_count = response.xpath("//tr[td[.='Badkamer']]/td[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        external_id = response.xpath("//p/strong[contains(.,'Referentie:')]/following::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        desc = "".join(response.xpath("//div/h3[contains(.,'Over dit')]/parent::div/p//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))
        available_date = ""
        if "beschikbaar" in desc:
            available_date = desc.split("beschikbaar")[0].split(".")[1].strip()
        elif "Beschikbaar" in desc:
            available_date = desc.split("Beschikbaar")[1].split(".")[0].replace("vanaf","").replace("begin","").strip()
            if "EPC" in available_date:
                available_date = available_date.split("EPC")[0].strip()
        
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                    
        energy_label = response.xpath("//ul/li/strong[contains(.,'EPC waarde')]/parent::li/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))
        
        parking = response.xpath("//tr/td[contains(.,'Garage') or contains(.,'Parking')]/following-sibling::td/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat:")[1].split(",")[0].strip()
            longitude = latitude_longitude.split("lng:")[1].split(",")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        images = [ x for x in response.xpath("//div[@class='uk-container']/h2[contains(.,'Fotogalerij')]/parent::div//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        elevator = response.xpath("//div/h3[contains(.,'Over dit')]/parent::div/ul//li[contains(.,'Lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        item_loader.add_value("landlord_name", "Immo maribrik BV")
        item_loader.add_value("landlord_phone", "+32 56 25 39 80")
        item_loader.add_value("landlord_email", "info@maribrik.be")
        
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

"""
        for prd in response.xpath(".//Property_Management_Overview"):
            url = prd.xpath(".//Property_URL/text()").get()
            property_type = ""
            if "/Woning" in url:
                property_type = "house"
            elif "/Appartement" in url:
                property_type = "apartment"
            if prd.xpath(".//Property_Reference/text()").get() in self.ids:
                continue
            self.ids.add(prd.xpath(".//Property_Reference/text()").get())
            if property_type:
                item_loader = ListingLoader(response=response)
                item_loader.add_value("property_type", property_type)
                item_loader.add_value("external_link", f"http://www.maribrik.be/NL{url}")
                item_loader.add_value(
                    "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
                )
                item_loader.add_value("rent_string", prd.xpath(".//Property_Price/text()").get())
                item_loader.add_value("latitude", prd.xpath(".//Property_Lat/text()").get())
                item_loader.add_value("longitude", prd.xpath(".//Property_Lon/text()").get())
                item_loader.add_value("description", prd.xpath(".//Property_Description/text()").get())
                item_loader.add_value("external_id", prd.xpath(".//Property_Reference/text()").get())
                item_loader.add_value("title", prd.xpath(".//Property_Title/text()").get())
                item_loader.add_value("city", prd.xpath(".//Property_City_Value/text()").get())
                item_loader.add_value("zipcode", prd.xpath(".//Property_Zip/text()").get())
                item_loader.add_value("room_count", prd.xpath(".//bedrooms/text()").get())
                # item_loader.add_value("external_id", prd.xpath(".//FortissimmoID").get())
                item_loader.add_value(
                    "address",
                    f'{prd.xpath(".//Property_Street/text()").get()} {prd.xpath(".//Property_Number/text()").get()}',
                )
                images = []
                base_images = "http://www.maribrik.be/fortissimmo/maribrik.be/images/"
                images.append(base_images + prd.xpath(".//Image_URL/text()").get())
                images.append(base_images + prd.xpath(".//Image_URL2/text()").get())
                images.append(base_images + prd.xpath(".//Image_URL3/text()").get())
                images.append(base_images + prd.xpath(".//Image_URL4/text()").get())
                item_loader.add_value("images", images)
                yield item_loader.load_item()
        """

