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
    name = 'alquilerprotegido_es'
    execution_type='testing'
    country='spain'
    locale='es'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.alquilerprotegido.es/inmuebles/?keyword=&offer=rent&location=&listing-type=casa-chalet&bedrooms=&bathrooms=&min=&max=&orderby=date&order=desc",
             "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//h2[@class='entry-title']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
        next_page = response.xpath("//ul[@class='page-numbers']/li/a/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", "Alquilerprotegido_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_xpath("title", "//h1[@class='entry-title']/text()")        
        item_loader.add_value("property_type", response.meta.get('property_type'))        
        item_loader.add_value("external_link", response.url)

        external_id=response.xpath("//h1[@class='entry-title']/text()").get().split("REF:")[1]
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)
        
        rent="".join(response.xpath("//div[@class='alignleft']/div/span/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
        
        address = response.xpath("//div[@class='wpsight-listing-district']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].strip())

        item_loader.add_xpath("zipcode", "normalize-space(//div[@class='wpsight-listing-id']/text())")
        
        room_count=response.xpath("//span[@title='Habitaciones']/span[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count=response.xpath("//span[span[contains(.,'Baños')]]/span[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters=response.xpath("//span[@class='listing-details-label'][contains(.,'Vivienda')]//following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].strip())
        
        desc="".join(response.xpath("//div[@class='wpsight-listing-description']//p//text()").get())
        if desc:
            item_loader.add_value("description", desc)
        latitude_longitude=response.xpath("//script[contains(.,'myLatlng')]/text()").get()
        if latitude_longitude:
            lat=latitude_longitude.split("LatLng(")[1].split(",")[0]
            lng=latitude_longitude.split("LatLng(")[1].split(",")[1].split(")")[0]
            if lat or lng:
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)

        images = [x for x in response.xpath("//div[@class='w-grid-list']/article//img/@src | //div[@class='wpsight-listing-image']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        parking = response.xpath("//span[span[contains(.,'Aparcamiento')]]/span[2]/text()").get()
        if parking and "si" in parking.lower():
            item_loader.add_value("parking", True)
        
        item_loader.add_value("landlord_name","ALQUILER")
        item_loader.add_value("landlord_phone","664 484 848")
        item_loader.add_value("landlord_email","info@alquilerprotegido.es")
            
        yield item_loader.load_item()









