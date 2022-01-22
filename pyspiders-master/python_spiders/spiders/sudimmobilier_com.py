# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from html.parser import HTMLParser
import dateparser


class MySpider(Spider):
    name = 'sudimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Sudimmobilier_PySpider_france_fr"
    def start_requests(self):
        start_urls = [
            {"url": "http://www.sudimmobilier.com/location-appartement-appartement-maison-canet-plage-roussillon.html", "property_type": "house"},
            {"url": "http://www.sudimmobilier.com/location-maison-appartement-maison-canet-plage-roussillon.html", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//article/div/div/div[@onclick]").extract():
            follow_url = response.urljoin(str(item).split("document.location='")[1].replace("'",""))
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        prop_type = response.meta.get('property_type')
        item_loader.add_value("property_type", prop_type)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        item_loader.add_value("external_link", response.url)
        external_id = response.xpath("//span[contains(.,'Réf:')]/text()").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
            item_loader.add_value("external_id", external_id)
        address =response.xpath("//span[contains(.,'Réf:')]/text()").get()
        if address:
            item_loader.add_value("address", address.split("/")[0].strip())
            item_loader.add_value("city", address.split("-")[0].strip())
            item_loader.add_value("zipcode", address.split("-")[1].split("/")[0].strip())
            
        square_meters = response.xpath("//span[.='Surface de :']/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])
        bathroom =response.xpath("//span[.='Salle de bains :']/following-sibling::span/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split("Salle(s)")[0])
        room_count = response.xpath("//span[.='Pièces :']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        utilities = response.xpath("//p[contains(.,'Honoraires charges locataire')]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].split(",")[0])

        deposit = response.xpath("//p[contains(.,'Dépôt de garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].split("€")[0].split(",")[0].replace(" ",""))
        price = response.xpath("//p[contains(.,'Loyer')]/text()").get()
        if price: 
            item_loader.add_value("rent", price.split("Loyer")[1].split("€")[0].split(",")[0])

        description = "".join(response.xpath("//p[@class='description-text description hide-on-booking mt-4']/text()").getall())
        if description:
            item_loader.add_value("description", description)

        images = [x for x in response.xpath("//a[@class='js-fancybox d-block u-block-hover u-block-hover--scale-down']//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)

        elevator = response.xpath("//span[.='Ascenseur :']/following-sibling::span/text()").get()
        if elevator and "oui"==elevator:
            item_loader.add_value("elevator", True)

        
        item_loader.add_value("landlord_name", "Sud Immobilier")
        item_loader.add_value("landlord_phone", "04 68 73 12 03")

        yield item_loader.load_item()

