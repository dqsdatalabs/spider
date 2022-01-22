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
    name = 'immobiliaremaggimilano_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Immobiliaremaggimilano_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.immobiliaremaggimilano.it/r/annunci/immobiliari.html?cf=yes&Motivazione%5B%5D=&Motivazione%5B%5D=2&commerciali=&milano=1&home_maggi=1&fuorimilano=&altrecitta=",
                ],
                #only milano
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
        for item in response.xpath("//ul[@class='realestate']/li/section//a//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, 
        )
        
        next_page = response.xpath("//div[@class='paging']/a[@class='next']//@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.xpath("//div[@class='box']/strong[contains(.,'Tipologia')]//parent::div/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return    
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//h1[@class='titoloscheda']//text()")
        external_id = response.xpath("//div[@class='codice_scheda']//span//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        else:
            external_id = response.xpath("//div[@class='box']/strong[contains(.,'Codice')]//parent::div/text()").get()
            item_loader.add_value("external_id", external_id.strip())

        rent = response.xpath("//div[@class='box']/strong[contains(.,'Prezzo')]//parent::div/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace(".","").split('€')[-1].strip())
        item_loader.add_value("currency", "EUR")
        desc=" ".join(response.xpath("//div[@class='testo']/p/text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        room_count = response.xpath("//div[@class='box']/strong[contains(.,'Camere')]//parent::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("(//div[@class='ico-24-camere']/span//text()[contains(.,'Camere')])[1]").get()
            item_loader.add_value("room_count", room_count.strip(' ')[0])
        bathroom_count = response.xpath("//div[@class='box']/strong[contains(.,'Bagni')]//parent::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        else:
            bathroom_count = response.xpath("(//div[@class='ico-24-bagni']/span//text()[contains(.,'Bagni')])[1]").get()
            item_loader.add_value("bathroom_count", bathroom_count.strip(' ')[0])

        square_meters = response.xpath("//div[@class='box']/strong[contains(.,'Totale mq')]//parent::div/text()").get()
        if square_meters:
            square_meters = square_meters.split('mq')[0].strip()
            square_meters = math.ceil(float(square_meters.strip()))
            item_loader.add_value("square_meters",square_meters)
        
        utilities = response.xpath("//div[@class='box']/strong[contains(.,'Spese condominio')]//parent::div/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace(".","").split('€')[-1].strip())
        floor = response.xpath("//div[@class='box']/strong[contains(.,'Piano')]//parent::div/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        city = response.xpath("//div[@class='box']/strong[contains(.,'Provincia')]//parent::div/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        address = response.xpath("//div[@class='box']/strong[contains(.,'Indirizzo')]//parent::div/text()").get()
        if address:
            address = address.strip()
            item_loader.add_value("address", address+ "," +city)
        
        latlng = response.xpath("//script//text()[contains(.,'var lat')]").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split('lat = "')[-1].split('";')[0])
            item_loader.add_value("longitude", latlng.split('lgt = "')[-1].split('";')[0])

        images = [x for x in response.xpath("(//div[@class='contenitoreGallerySchedaImmo']/div//div[@class='swiper-wrapper'])[1]/div/img/@data-src").extract()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        energy_label = response.xpath("//div[@class='classe_energ']/div//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        furnished = response.xpath("//div[@class='box']/strong[contains(.,'Arredato')]//parent::div/text()").get()
        if furnished and "sì" in furnished.lower().strip():
            item_loader.add_value("furnished", True)
        elif furnished and "parzialmente arredato" in furnished.lower().strip():
            item_loader.add_value("furnished", True)
        elevator = response.xpath("//div[@class='box']/strong[contains(.,'Ascensore')]//parent::div/text()").get()
        if elevator and "si" in elevator.lower().strip():
            item_loader.add_value("elevator", True)
        balcony = response.xpath("//div[@class='box']/strong[contains(.,'Balconi')]//parent::div/text()").get()
        if balcony and "presente" in balcony.lower().strip():
            item_loader.add_value("balcony", True)
           
        item_loader.add_value("landlord_name", "Studio Maggi")
        item_loader.add_value("landlord_phone", "02861941")
        item_loader.add_value("landlord_email", "info@maggimilano.it")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "appartamento" in p_type_string.lower():
        return "apartment"
    elif p_type_string and ("loft / open space" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "apartment"
    else:
        return None