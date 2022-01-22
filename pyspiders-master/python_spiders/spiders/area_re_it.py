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
    name = 'area_re_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = 'Area_Re_PySpider_italy'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.area-re.it/immobili/?1=1&sImmobileOfferta=locazione",
                ]
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                )
 
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='col-sm-12 nopadding immobile-item-title']/h3/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        prop_type = response.xpath("//div[@class='col-sm-12 immobile-description nopadding']/div/strong[contains(.,'Tipologia:')]/following-sibling::span/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
     
        external_id = response.xpath("//strong[contains(.,'Codice:')]/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//div[contains(@class,'immobile-title')]/h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[-1].strip())

        item_loader.add_value("currency", "EUR")
        address=response.xpath("//h1//text()").get()
        if address:
            item_loader.add_value("address",address)
        item_loader.add_value("city","Rome")

        utilities = response.xpath("//strong[contains(.,'Condominio')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[-1].strip())

        square_meters = response.xpath("//strong[contains(.,'MQ:')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        floor = response.xpath("//strong[contains(.,'Piano:')]/following-sibling::span/text()").get()
        if floor:
            floor = floor.strip()
            if floor.isdigit():
                item_loader.add_value("floor", floor)
        
        balcony = response.xpath("(//strong[contains(.,'Balconi:')]/following-sibling::span/text())[2]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = response.xpath("//strong[contains(.,'Ascensore:')]/following-sibling::span/i[contains(@class,'fa fa-check')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        parking = response.xpath("//strong[contains(.,'Box Auto:')]/following-sibling::span/text()").get()
        if parking and "no" not in parking.lower():
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//strong[contains(.,'Posto Auto:')]/following-sibling::span/text()").get()
            if parking and "no" not in parking.lower():
                item_loader.add_value("parking", True)
        
        desc = "".join(response.xpath("//div[@class='col-sm-12 immobile-description']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc)
        
        latlng = response.xpath("//script[@type='text/javascript']/text()[contains(.,'centerLongLat')]").get()
        if latlng:
            latitude = latlng.split('lat:')[-1].split(',')[0].strip()
            item_loader.add_value("latitude", latitude)
            longitude = latlng.split('lng:')[-1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
        
        images = [x for x in response.xpath("//ul[@id='lightSlider']/li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        floor_plan_images = [x for x in response.xpath("//div[@class='col-sm-4 padding0-xs']/a/img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        landlord_name = response.xpath("(//ul[@class='col-sm-12 col-xs-12 container-other-profile-item']/li/a/text())[1]").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        landlord_email = response.xpath("//ul[@class='col-sm-12 col-xs-12 container-other-profile-item']/li/a/text()[contains(.,'@')]").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.strip())
        landlord_phone = response.xpath("//ul[@class='col-sm-12 col-xs-12 container-other-profile-item']/li//a/text()[contains(.,'Tel')]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.split('Tel:')[-1].strip())
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "appartamento" in p_type_string.lower():
        return "apartment"
    elif p_type_string and "attico" in p_type_string.lower():
        return "apartment"
    elif p_type_string and "casa indipendente" in p_type_string.lower():
        return "house"
    else:
        return None