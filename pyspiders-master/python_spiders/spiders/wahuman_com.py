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

class MySpider(Spider):
    name = 'wahuman_com'
    execution_type='testing'
    country='france'
    locale='fr'
    scale_separator='.'
    external_source= "Wahuman_PySpider_france_fr"
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.wahuman.com/a-louer/1",
                ],
            },

        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                )
            
    # 1. FOLLOWING
    def parse(self, response): 
        for item in response.xpath("//div[@class='card-bottom']/div/a/@href").extract():
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
        item_loader.add_xpath("title","//div/h1[@class='detail-title']//text()[normalize-space()]")
        item_loader.add_value("external_source", self.external_source)

        address = response.xpath("//tr/th[contains(.,'Ville')]/following-sibling::th/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        zipcode = response.xpath("//tr/th[contains(.,'Code')]/following-sibling::th/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        latitude_longitude = response.xpath("//script[contains(.,'getMap')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat : ')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:  ')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        square_meters = response.xpath("//th[contains(.,'Surface habitable (m²)')]/following-sibling::th/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip()
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//th[contains(.,'pièce')]/following-sibling::th/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//th[contains(.,'Nb de salle de bains')]/following-sibling::th/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//th[contains(.,'Loyer')]/following-sibling::th/text()").get()
        if rent:
            rent = rent.strip().replace(' ', '').replace(".","")
            item_loader.add_value("rent_string", rent)

        external_id = response.xpath("//b[contains(.,'Ref')]/parent::span/following-sibling::text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//p[@itemprop='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        images = [urljoin('https://www.wahuman.com', x) for x in response.xpath("//ul[@class='imageGallery notLoaded']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//th[contains(.,'Dépôt de garantie')]/following-sibling::th/text()").get()
        if deposit:
            deposit = deposit.split('€')[0].strip().replace(' ', '')
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//th[contains(.,'Charges locatives')]/following-sibling::th/text()").get()
        if utilities:
            utilities = utilities.split('€')[0].strip().replace(' ', '')
            item_loader.add_value("utilities", utilities)

        furnished = response.xpath("//th[contains(.,'Meublé')]/following-sibling::th/text()").get()
        if furnished:
            if furnished.strip().lower() == 'non':
                furnished = False
            elif furnished.strip().lower() == 'oui':
                furnished = True
            if type(furnished) == bool:
                item_loader.add_value("furnished", furnished)

        floor = response.xpath("//th[contains(.,'Etage')]/following-sibling::th/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        elevator = response.xpath("//th[contains(.,'Ascenseur')]/following-sibling::th/text()").get()
        if elevator:
            if elevator.strip().lower() == 'non':
                elevator = False
            elif elevator.strip().lower() == 'oui':
                elevator = True
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//th[contains(.,'Balcon')]/following-sibling::th/text()").get()
        if balcony:
            if balcony.strip().lower() == 'non':
                balcony = False
            elif balcony.strip().lower() == 'oui':
                balcony = True
            if type(balcony) == bool:
                item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//th[contains(.,'Terrasse')]/following-sibling::th/text()").get()
        if terrace:
            if terrace.strip().lower() == 'non':
                terrace = False
            elif terrace.strip().lower() == 'oui':
                terrace = True
            if type(terrace) == bool:
                item_loader.add_value("terrace", terrace)

        item_loader.add_value("landlord_name", 'WAH MONTPELLİER')
        item_loader.add_value("landlord_phone", '04 67 10 94 19')

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data
