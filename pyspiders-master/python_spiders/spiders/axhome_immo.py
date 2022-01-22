# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math


class MySpider(Spider):
    name = 'axhome_immo'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.axhome.immo/recherche?a=2&b%5B%5D=appt&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.axhome.immo/recherche?a=2&b%5B%5D=house&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher",
                "property_type" : "house"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 12)

        seen = False
        for item in response.xpath("//span[@class='text_detail']/../@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={
                    'property_type' : response.meta.get('property_type'),
                }
            )
            seen = True
        
        if page == 12 or seen:
            try:
                p_url = response.url.split("&start")[0] + f"&start={page}" + (response.url.split("&start")[1])[response.url.split("&start")[1].find("&"):]
            except:
                return
            yield Request(
                p_url, 
                callback=self.parse, 
                meta={
                    'property_type' : response.meta.get('property_type'),
                    "page" : page+12
                }
            )
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        #NOT:: SUANDA RENT YOK SPIDER BITINCE PROP_FOR_SALE DICT'I SILMEYI UNUTMAYALIM...

        item_loader.add_value("external_source", "Axhome_immo_PySpider_"+ self.country + "_" + self.locale)

        title=response.xpath("//div[@id='page_title']/h1/text()").extract_first()
        if title:
            item_loader.add_value("title",title)

        utilities=response.xpath("//tr[td[.='Charges']]/td[2]/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities",utilities.split(" ")[0].strip())

        price = response.xpath("//div[@id='value_prod']//text()[normalize-space()]").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))

        item_loader.add_xpath("external_id", "//tr/td[contains(.,'Référence')]/following-sibling::td/text()")
        item_loader.add_xpath("address", "//tr/td[contains(.,'Ville')]/following-sibling::td//span[1]/text()")
        item_loader.add_xpath("city", "//tr/td[contains(.,'Ville')]/following-sibling::td//span[1]/text()")
        item_loader.add_xpath("room_count", "//tr/td[contains(.,'Chambres')]/following-sibling::td/text()")
        item_loader.add_xpath("bathroom_count", "//tr/td[contains(.,'Salle de bains')]/following-sibling::td/text()")
        item_loader.add_xpath("floor", "//tr/td[contains(.,'Étage')]/following-sibling::td/text()")
        
        zipcode = response.xpath("//tr/td[contains(.,'Ville')]/following-sibling::td//span[2]/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip() )
        square = response.xpath("//tr/td[contains(.,'Surface')]/following-sibling::td/text()").extract_first()
        if square:
            square_meters = math.ceil(float(square.replace("m²","").strip()))
            item_loader.add_value("square_meters",square_meters )
           
        desc = "".join(response.xpath("//div[@id='details']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        terrace = response.xpath("//tr/td[contains(.,'Terrasse')]/text()").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//tr/td[contains(.,'Stationnement')]/following-sibling::td/text()").extract_first()
        if parking:
            if "Non" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        furnished = response.xpath("//tr/td[contains(.,'Vue')]/following-sibling::td/text()").extract_first()
        if furnished:
            if "Dégagée" in furnished:
                item_loader.add_value("furnished", False)
       
        elevator = response.xpath("//tr/td[contains(.,'Ascenseur')]/following-sibling::td/text()").extract_first()
        if elevator:
            if "Non" in elevator:
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        
        swimming_pool = response.xpath("//tr/td[contains(.,'Piscine')]/following-sibling::td/text()").extract_first()
        if swimming_pool:
            if "Non" in swimming_pool:
                item_loader.add_value("swimming_pool", False)
            else:
                item_loader.add_value("swimming_pool", True)

        map_coordinate = response.xpath("//script[contains(.,'setView')]/text()").extract_first()
        if map_coordinate:
            lat_lng=map_coordinate.split('setView([')[1].split(']')[0]
            lat = lat_lng.split(',')[0].strip()
            lng = lat_lng.split(',')[1].strip()
            if lat:
                item_loader.add_value("longitude", lng)
            if lng:
                item_loader.add_value("latitude", lat)
       
        images = [x for x in response.xpath("//a[@class='rsImg']//img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "03 88 61 37 09")
        item_loader.add_value("landlord_name", "AX'HOME IMMOBILIER")
        item_loader.add_value("landlord_email", "axhome@axhome.net")

        yield item_loader.load_item()
