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
    name = 'agenziaregilla_it'
    execution_type = 'testing'
    country = 'italy' 
    locale = 'it'
    external_source = "Agenziaregilla_Pyspider_italy"
    start_urls = ['http://www.agenziaregilla.it/property/?post_type=property&search_keyword=&status=For+Rent&price-min=&price-max=&city=&state=&zip=&beds=&baths=&ptype=&sqft=&submit=Aramak']  # LEVEL 1
    custom_settings = {
      "PROXY_TR_ON": True,
    }
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//a[contains(.,'Vedi Annuncio')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"http://www.agenziaregilla.it/property/page/{page}/?post_type=property&search_keyword&status=For%20Rent&price-min&price-max&city&state&zip&beds&baths&ptype&sqft&submit=Aramak"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        prop_type = response.xpath("//li[contains(.,'Tipologia')]/span/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath(
            "//div[contains(@class,'property-meta-single')]//div[contains(@class,'listings-address-widget')]//text()").get()
        if title:
            item_loader.add_value("title", title)
        external_id=response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split("p=")[-1])

        address = response.xpath(
            "//div[contains(@class,'property-meta-single')]//div[contains(@class,'listings-street-widget')]//text()").get()
        if address:
            item_loader.add_value("address", address)
            city=address.split(",")[:1]
            item_loader.add_value("city", city)

        description = response.xpath(
            "//div[contains(@class,'property-listing-single')]//following-sibling::p//text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath(
            "//h2[contains(@class,'title-bg')]//following-sibling::span//text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[1].split(",")[0])
        item_loader.add_value("currency", "EUR")
        zipcode=response.xpath("//div[@class='listings-street-widget']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(",")[-1])
        room_count=response.xpath("//li[contains(.,'Camere')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        
        bathroom_count=response.xpath("//li[contains(.,'Bagni')]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        utilities=response.xpath("//li[contains(.,'Spese Condominio')]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[-1])
        elevator=response.xpath("//li[contains(.,'Ascensore')]/span/text()").get()
        if elevator:
            if "No" in elevator:
                item_loader.add_value("elevator",False)
            if "Si" in elevator:
                item_loader.add_value("elevator",True)
            

        square_meters = response.xpath(
            "//ul[contains(@id,'house-details-sidebar')]//li[contains(.,'Dimensione:')]//span//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("mq"))

        images = [response.urljoin(x) for x in response.xpath("//ul//li//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'latitude')]//text()").get()
        if latitude_longitude:
            longitude = latitude_longitude.split('latitude:')[1].split(',')[0]
            latitude = latitude_longitude.split('longitude:')[1].split(',')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        name=response.xpath("//div[@class='agent-details']/h5/a/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//div[@class='agent-details']/ul/li[2]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)



        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower() or "abitazione" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "loft" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    else:
        return None