# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'tore_es'
    execution_type = 'testing'
    country = 'spain'
    locale ='es'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.inmotore.es/alquiler.php?limtipos=2799,4299,3399&buscador=1",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.inmotore.es/alquiler.php?limtipos=299,5799,2999,899,399,3299,6499,2899,4399,199,3599,4999,6299,6599,6899&buscador=1"
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://www.inmotore.es/alquiler.php?limtipos=3099&buscador=1",
                ],
                "property_type": "studio"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={
                        'property_type': url.get('property_type'),
                        'base_url': item
                    }
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        base_url = response.meta.get('base_url')
        seen = False

        for item in response.xpath("//article[contains(@class,'paginacion-ficha')]"):
            follow_url = response.urljoin(item.xpath(".//a[@class='irAfichaPropiedad']/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"{base_url}&pag={page}"
            yield Request(
                url,
                callback=self.parse,
                meta={
                    "page": page+1,
                    "property_type": response.meta.get('property_type'),
                    "base_url": base_url
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Tore_PySpider_spain")

        title = response.xpath("//div[contains(@class,'tituloprincipal')]/h1/text()").get()
        item_loader.add_value("title", title)
        
        room_count = response.xpath("//li[@title='Bedrooms']/text()").get()
        item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[@title='Bathrooms']/text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)
                
        rent = response.xpath("//div[contains(@class,'fichapropiedad-precio')]/text()").get()
        if rent:
            rent = rent.split(" ")[0].replace(".","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        images = [x for x in response.xpath("//div[@class='visorficha-miniaturas']//@cargafoto").getall()]
        if images:
            item_loader.add_value("images", images)
        
        description = "".join(response.xpath("//section[@id='fichapropiedad-bloquedescripcion']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        external_id = response.xpath("//li[span[contains(.,'Reference')]]/span[2]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//li[span[contains(.,'City')]]/span[2]/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(" / ")[-1])

        
        square_meters = response.xpath("//li[span[contains(.,'Net') or contains(.,'Built')]]/span[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])
        
        swimming_pool = response.xpath("//li[i[contains(@class,'check')]]/b/text()[contains(.,'Pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        furnished = response.xpath("//li[i[contains(@class,'check')]]/b/text()[contains(.,'Furniture')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        parking = response.xpath("//li[span[contains(.,'parking')]]/span[2]/text() | //li[@class='parking']//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        energy_label = response.xpath("//div[@class='flechaEficiencia']/text()").get()
        item_loader.add_value("energy_label", energy_label)
        
        latitude = response.xpath("//script[contains(.,'latitud') and contains(.,'listados.fichapropiedad') ]/text()").get()
        if latitude:
            lat = latitude.split('"latitud":')[-1].split(",")[0]
            lng = latitude.split('"altitud":')[-1].split(",")[0]
            item_loader.add_value("latitude", lat.replace('"',''))
            item_loader.add_value("longitude", lng.replace('"',''))
        
        item_loader.add_value("landlord_name", "Inmobiliaria TORÉ")
        item_loader.add_value("landlord_phone", "952 600 004")
        item_loader.add_value("landlord_email", "info@tore.es")
        
        yield item_loader.load_item()