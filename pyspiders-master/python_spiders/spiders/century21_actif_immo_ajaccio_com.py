# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import extract_number_only, remove_white_spaces, format_date
import re
import locale
import lxml,js2xml
from parsel import Selector

class Century21actifimmoajaccioSpider(scrapy.Spider):
    name = "century21_actif_immo_ajaccio_com"
    allowed_domains = ["century21-actif-immo-ajaccio.com"]
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    external_source = "Century21_Actif_Immo_Ajaccio_PySpider_france_fr"
    thousand_separator=' '
    scale_separator=','
    position=0
    
    def start_requests(self):
        start_urls = [            
            {'url': 'https://www.century21-actif-immo-ajaccio.com/annonces/location-appartement/page-1/',
                'property_type': 'apartment'},
            {'url': 'https://www.century21-actif-immo-ajaccio.com/annonces/location-maison/page-1/',
                'property_type': 'house'}
            ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url':url.get('url'),
                                       'property_type':url.get('property_type')})
            
    def parse(self, response, **kwargs):
        for property_url in response.xpath('//a[@data-name="view_property"]/@href').extract():
            yield scrapy.Request(
                response.urljoin(property_url),
                callback=self.get_property_details,
                meta={'request_url': response.urljoin(property_url),
                      "property_type": response.meta["property_type"]})
        
        if len(response.xpath('.//a[@class="tw-block"]')) > 0:
            current_page = re.findall(r"(?<=page-)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=page-)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      "property_type": response.meta["property_type"]}
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value('external_link',response.meta.get('request_url'))
        item_loader.add_value('property_type',response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        
        
        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)
        external_id = response.xpath("//div[contains(@class,'c-text-theme-cta')]/text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)
   
        description = response.xpath("//div[contains(@class,'has-formated-text')]/text()").getall()
        if description:
            item_loader.add_value("description", "".join(description).strip())
        
        item_loader.add_xpath("latitude", "//div/@data-lat")
        item_loader.add_xpath("longitude", "//div/@data-lng")

        
        address = response.xpath("//h1/span[last()]/text()").get()
        if address:    
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(" -")[0])
            
        zipcode = response.xpath("//li[@itemprop='itemListElement'][last()]//span[@itemprop='name']/text()").get()
        if zipcode:        
            item_loader.add_value("zipcode", zipcode.split("(")[1].split(")")[0].strip())
        
        square_meters = response.xpath("//li[contains(.,'Surface habitable')]/text()").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m")[0].split(",")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//li[contains(.,'Nombre de pièces')]/text()").get()
        if room_count:
            room_count = room_count.split(":")[1].strip()
        else:
            if title and "pièces" in title.lower():
                for i in title.split(" - "):
                    if "pièces" in i:
                        room_count = i.split(" ")[0].strip()
                        break
        item_loader.add_value("room_count", room_count)

        images = response.xpath("//div/@x-data[contains(.,'image')]").get()
        if images:
            images = images.split("source: '")
            for i in range(1,len(images)):
                item_loader.add_value("images", response.urljoin(images[i].split("'")[0]))
        
        price = response.xpath("//p[contains(@class,'price ')]/text()").get()
        if price:
            item_loader.add_value("rent_string", price.replace("\xa0","").replace(" ","").strip())

            # price = price.split("/")[0].split("€")[0].strip().replace(" ","")
            # item_loader.add_value("rent", price)
            # item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].strip("€").replace(" ",""))
        
        utilities = response.xpath("//li/p[contains(.,'charges')]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split(",")[0].replace(" ",""))
        
        parking = response.xpath("//p[@class='tw-leading-none' and contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//li[contains(.,'meublée')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        terrace = response.xpath("//p[@class='tw-leading-none' and contains(.,'Terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//li[contains(.,'Ascenseur')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        energy_label = response.xpath("//span[contains(@class,'tw-block tw-m-auto')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(energy_label.strip().replace(",",".")))
        
        item_loader.add_value('landlord_phone','04 95 21 18 00')
        item_loader.add_value('landlord_email','actifimmo-ajaccio@century21.fr')
        item_loader.add_value('landlord_name','CENTURY 21 Actif Immobilier, Agence immobilière')
        
        item_loader.add_value("currency","EUR")

        self.position+=1
        item_loader.add_value('position',self.position)
        yield item_loader.load_item()
        
def energy_label_calculate(energy_number):
    energy_number = int(float(energy_number))
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label