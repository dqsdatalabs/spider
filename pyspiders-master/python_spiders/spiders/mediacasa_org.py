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
    name = 'mediacasa_org'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Mediacasa_PySpider_italy"
    start_urls = ['https://www.mediacasa.org/tutti-gli-immobili/']  # LEVEL 1
    
    formdata = {
        "searchTipoContr": "104",
        "searchCitta": "",
        "searchQuartiere": "",
        "searchBtn": "Cerca",
        "searchTipoImm": "22",
        "searchNumLocali": "",
        "priceMin": "€0",
        "priceMax": "€450,000",
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "22",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "194", "196", "26", "30"
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                self.formdata["searchTipoImm"] = item
                yield FormRequest(
                    url=self.start_urls[0],
                    dont_filter=True,
                    callback=self.parse,
                    formdata=self.formdata,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'Dettagli')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(@title,'successiva')]/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath(
            "//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        city = response.xpath(
            "//div[@class='property-title']//span//a[@class='listing-address']//i//following-sibling::text()").get()
        if city:
            item_loader.add_value("city", city.replace("\r","").replace("\n","").replace("\t","").replace(" ",""))

        description = response.xpath(
            "//h3[contains(.,'Descrizione')]//following-sibling::div//p//text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath(
            "//div[contains(@class,'property-pricing')]//div[contains(@class,'property-price')]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€"))
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath(
            "//ul[contains(@class,'property-features margin-top-0')]//li[contains(.,'Superficie: ')]//span//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        bathroom_count = response.xpath(
            "//ul[contains(@class,'property-features margin-top-0')]//li[contains(.,'Bagni:')]//span//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        room_count = response.xpath(
            "//ul[contains(@class,'property-features margin-top-0')]//li[contains(.,'Locali')]//span//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        utilities = response.xpath(
            "//ul[contains(@class,'property-features margin-top-0')]//li[contains(.,'mensili:')]//span//text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities)

        terrace = response.xpath(
            "//ul/li/text()[contains(.,'Terrazzo')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        else:
            item_loader.add_value("terrace", False)
        
        parking = response.xpath(
            "//ul/li/text()[contains(.,'Posto Auto')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)
        
        elevator = response.xpath(
            "//ul/li/text()[contains(.,'Ascensore')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        else:
            item_loader.add_value("elevator", False)
        
        furnished = response.xpath(
            "//ul/li/text()[contains(.,'Arredato')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)
        
        balcony = response.xpath(
            "//ul/li/text()[contains(.,'Balcone')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)

        images = [response.urljoin(x) for x in response.xpath(
            "//div[@class='property-slider default']//a[contains(@class,'item mfp-gallery')]//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Media Casa")
        item_loader.add_value("landlord_phone", "0630893724")
        item_loader.add_value(
            "landlord_email", "info@mediacasa.org")

        status = response.xpath("//div[@class='sub-price']/text()").get()
        if status and "affitto" in status.lower():
            map_iframe = response.xpath("//iframe[contains(@src,'maps/embed')]/@src").get()
            if map_iframe: 
                yield Request(map_iframe, callback=self.get_map, dont_filter=True, meta={"item_loader": item_loader})
            else:
                address = response.xpath("(//a[@class='listing-address']/text())[2]").get()
                if address:
                    address = address.split('Via')[-1].strip()
                    item_loader.add_value("address", address)
                
                yield item_loader.load_item()
        

    def get_map(self, response):
        item_loader = response.meta["item_loader"]
        latitude = response.xpath("//div[@id='mapDiv']/following-sibling::script[1]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('",null,[null,null,')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('",null,[null,null,')[1].split(',')[1].split(']')[0].strip())

        address = "".join(response.xpath("//script/text()[contains(.,'onEmbedLoad')]").extract())
        if address:
            address = address.split(',null,null,null,null,null,null,null,null,null,"')[-1].split('","Via ')[-1].split('",null,null,null,null,')[0].strip()
            if address and "via" in address.lower():
                item_loader.add_value("address", address.split('Via')[-1].strip())
            else:
                item_loader.add_value("address", address)
        
            zip = address.split('Via')[-1].strip()
            if zip and "," in zip:
                if zip.count(',') == 2:
                    zipcode = zip.split(', ')[2].split(' ')[0].strip()
                    item_loader.add_value("zipcode", zipcode)
                elif zip.count(',') == 3:
                    zipcode = zip.split(', ')[2].split(' ')[0].strip()
                    item_loader.add_value("zipcode", zipcode)
            else:
                zipcode = zip.split(' ')[0].strip()
                item_loader.add_value("zipcode", zipcode)
       
       
        yield item_loader.load_item()