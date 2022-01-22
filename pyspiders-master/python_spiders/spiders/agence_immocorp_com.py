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
    name = 'agence_immocorp_com'
    execution_type='testing'
    country='france'
    locale='fr'
    url = "https://www.agence-immocorp.com/recherche/"
    
    def start_requests(self):

        start_urls = [
            {
                "formdata" : {
                    'data[Search][offredem]': '2',
                    'data[Search][idtype]':'2'
                },
                "property_type" : "apartment",
            },      

        ] #LEVEL-1

        for item in start_urls:
            yield FormRequest(self.url,
                            formdata=item["formdata"],
                            dont_filter=True,
                            callback=self.parse,
                            meta={"property_type": item["property_type"], "formdata": item["formdata"]})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//h1[contains(@itemprop,'name')]//@href").extract():
            print(item)
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Agenceimmocorp_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        address = response.xpath("//span[contains(.,'Ville')]//following-sibling::span//text()").get()
        if address:
            address = address.strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)

        zipcode = response.xpath("//span[contains(.,'Code postal')]//following-sibling::span//text()").get()
        if zipcode:
            zipcode = zipcode.strip()
            item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//span[contains(.,'Surface habitable')]//following-sibling::span//text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip().replace(',', '.'))))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//span[contains(.,'chambre')]//following-sibling::span//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//span[contains(.,'pièces')]//following-sibling::span//text()").get()
            if room_count:
                room_count = room_count.strip()
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(.,'salle')]//following-sibling::span//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//span[contains(.,'Loyer')]//following-sibling::span//text()").get()
        if rent:
            rent = rent.strip().replace('\xa0', '').replace('€','').replace(' ', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        external_id = response.xpath("//span[contains(@class,'ref')]//text()").get()
        if external_id:
            external_id = external_id.strip().split(" ")[-1]
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//p[@itemprop='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        images = [x for x in response.xpath("//ul[contains(@class,'imageGallery')]//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//span[contains(.,'garantie')]//following-sibling::span//text()").get()
        if deposit:
            deposit = deposit.split('€')[0].strip().replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("deposit", deposit)  

        utilities = response.xpath("//span[contains(.,'Charges')]//following-sibling::span//text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        floor = response.xpath("//span[contains(.,'Etage')]//following-sibling::span//text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        elevator = response.xpath("//span[contains(.,'Ascenseur')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        balcony = response.xpath("//span[contains(.,'Balcon')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        terrace = response.xpath("//span[contains(.,'Terrasse')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//span[contains(.,'Meublé')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        parking = response.xpath("//span[contains(.,'garage')]//following-sibling::span//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "IMMOCORP")
        item_loader.add_value("landlord_phone", "01 75 18 34 56")
        item_loader.add_value("landlord_email", "contact@agence-immocorp.com")

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data
