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

class MySpider(Spider):
    name = 'firstestates_es'
    execution_type='testing'
    country='spain'
    locale='es'
    external_source='Firstestates_PySpider_spain_es'
    thousand_separator =","
    scale_separator ="."
    
    def start_requests(self):
        start_urls = [
            { "url": "https://firstestates.es/property-search/?il_page=1&ref_no=&listing_type=long_rental&location=0&type=33828&bedrooms_min=&bathrooms_min=&list_price_min=&list_price_max=&order=",
             "property_type": "studio" },

            { "url": "https://firstestates.es/property-search/?il_page=1&ref_no=&listing_type=long_rental&location=0&type=33830&bedrooms_min=&bathrooms_min=&list_price_min=&list_price_max=&order=",
             "property_type": "studio" },

            { "url": "https://firstestates.es/property-search/?il_page=1&ref_no=&listing_type=long_rental&location=0&type=33832&bedrooms_min=&bathrooms_min=&list_price_min=&list_price_max=&order=",
             "property_type": "studio" },

            { "url": "https://firstestates.es/property-search/?il_page=1&ref_no&listing_type=long_rental&location=0&type=33820&bedrooms_min&bathrooms_min&list_price_min&list_price_max&order",
             "property_type": "apartment" },

            { "url": "https://firstestates.es/property-search/?il_page=1&ref_no=&listing_type=long_rental&location=0&type=33822&bedrooms_min=&bathrooms_min=&list_price_min=&list_price_max=&order=",
             "property_type": "apartment" },

            { "url": "https://firstestates.es/property-search/?il_page=1&ref_no=&listing_type=long_rental&location=0&type=33824&bedrooms_min=&bathrooms_min=&list_price_min=&list_price_max=&order=",
             "property_type": "apartment" },

            { "url": "https://firstestates.es/property-search/?il_page=1&ref_no=&listing_type=long_rental&location=0&type=33826&bedrooms_min=&bathrooms_min=&list_price_min=&list_price_max=&order=",
             "property_type": "apartment" },

            { "url": "https://firstestates.es/property-search/?il_page=1&ref_no=&listing_type=long_rental&location=0&type=33834&bedrooms_min=&bathrooms_min=&list_price_min=&list_price_max=&order=",
             "property_type": "house" }
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={"property_type":url.get('property_type')})
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='row ro_lst']//button/@data-ref").extract():
            follow_url = f"/es/propiedad/?ref_no={item}"
            yield Request(response.urljoin(follow_url), callback=self.populate_item, meta={'property_type': response.meta.get("property_type")})

        next_page = response.xpath("//div[@class='row ro_lst']//a[@class='next page-numbers']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Firstestates_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("(//div[@class='et_pb_text_inner']/span/text())[2]").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        prop_type = response.xpath("//div[contains(text(),'Tipo de Propiedad')]/following-sibling::div[1]//text()").get()
        if prop_type:
            if "estudio" in prop_type.lower():
                item_loader.add_value("property_type", 'studio')
            elif "apartamento" in prop_type.lower():
                item_loader.add_value("property_type", 'apartment')
            elif "casa" in prop_type.lower() or "villa" in prop_type.lower() or "bungalow" in prop_type.lower() or "cortijo" in prop_type.lower():
                item_loader.add_value("property_type", "house")
            else: item_loader.add_value("property_type", "house")
        else: return
        
        item_loader.add_value("external_link", response.url)

        bathroom_count = response.xpath("//div[contains(text(),'Baños')]/following-sibling::div[1]//text()").get()
        if bathroom_count: item_loader.add_value("bathroom_count", bathroom_count)

        meters = "".join(response.xpath("//div[contains(text(),'Tamaño Construido')]/following-sibling::div[1]//text()").getall())
        if meters:
            item_loader.add_value("square_meters", meters.split("m")[0].strip())
        
        room_count = response.xpath("//div[contains(text(),'Dormitorios')]/following-sibling::div[1]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        external_id = response.xpath("//div[contains(text(),'Referencia')]/following-sibling::div[1]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = ""
        area = response.xpath("//div[contains(text(),'Ubicación')]/following-sibling::div[1]//text()").get()
        if area: address += area.strip() + " "

        city = response.xpath("//div[contains(text(),'Zona')]/following-sibling::div[1]//text()").get()
        if city:
            address += city.strip() + " "
            item_loader.add_value("city", city.strip())
        
        if address:
            item_loader.add_value("address", address.strip())

        images = [response.urljoin(x)for x in response.xpath("//ul[@id='image-gallery']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        price = response.xpath("//div[contains(text(),'Precio')]/following-sibling::div[1]/span/span/@data-price").get()
        if price :
            item_loader.add_value("rent", price.strip())
            item_loader.add_value("currency", 'EUR')
            

        desc = " ".join(response.xpath("//span[contains(@class,'descp own')]//text() | //span[contains(@class,'descp')]//text()").getall()).strip()
        item_loader.add_value("description", desc)

        terrace = response.xpath("//li[@class='features']//p[contains(.,'Terraza')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        elif response.xpath("//div[contains(text(),'Terraza')]/following-sibling::div[1]//text()").get():
            if int(response.xpath("//div[contains(text(),'Terraza')]/following-sibling::div[1]//text()").get().split('m')[0].replace(',','.')) > 0: item_loader.add_value("terrace", True)

        elevator = response.xpath("//li[@class='features']//p[contains(.,'Ascensor')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        furnished = response.xpath("//li[@class='features']//p[contains(.,'Amueblada')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        washing_machine = response.xpath("//li[@class='features']//p[contains(.,'Lavadora')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        parking = response.xpath("//li[@class='features']//h4[contains(.,'Estacionamiento') or contains(.,'Garaje')]").get()
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_value("landlord_phone", "34 951 041 541")
        item_loader.add_value("landlord_email", "info@firstestates.es")
        item_loader.add_value("landlord_name", "Firstestates")

        yield item_loader.load_item()