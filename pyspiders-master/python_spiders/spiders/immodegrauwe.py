# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from scrapy import Request
from datetime import datetime
from ..loaders import ListingLoader
from ..helper import extract_number_only, extract_rent_currency, format_date
from ..user_agents import random_user_agent
from datetime import date
import re


class ImmodegrauweSpider(scrapy.Spider):
    name = 'immodegrauwe_be'
    allowed_domains = ['www.immodegrauwe.be']
    start_urls = ['http://www.immodegrauwe.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    position = 0
    thousand_separator = '.'
    scale_separator = ','
    external_source="Immodegrauwe_PySpider_belgium_nl"
    #https://www.immodegrauwe.be -->>>https://www.immo-zone.be/
    def start_requests(self):
        start_urls = [{
            'url': 'https://www.immo-zone.be/huren/alle/woning/alle/alle',
            'property_type': 'house'
            },
            {
            'url': 'https://www.immo-zone.be/huren/alle/appartement/alle/alle',
            'property_type': 'apartment'
            }
        ]

        for url in start_urls:
            yield Request(url=url.get('url'),
                          callback=self.parse, 
                          meta={'request_url': url.get('url'),
                                'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        for item in response.xpath("//a[@class='overlay']/@href").getall():
            follow_url=response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,dont_filter=True, meta={"property_type":response.meta["property_type"]})

    def populate_item(self,  response):
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        external_id = response.xpath("//li[contains(.,'Referentie')]/span/text()").get()
        if external_id:
            item_loader.add_value('external_id', external_id)
        title=response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value('title', title)
        rent=response.xpath("//span[@class='price']/text()").get()
        if rent:
            rent=rent.replace("€","").split(" ")[-1]
            item_loader.add_value('rent', rent)

        # if 'Alg. onkosten' in response.xpath('//span[@class="p_prijs"]/text()').extract_first():
        #     item_loader.add_value('utilities', extract_number_only(response.xpath('//span[@class="p_prijs"]/text()').extract_first().split('|')[-1])[:-2])

        item_loader.add_value('property_type', response.meta.get('property_type'))
        images=response.xpath("//a[@data-lightbox='pubimages']/@href").getall()
        if images:
            item_loader.add_value('images', images)
        square_meters=response.xpath("//li[contains(.,'Woonoppervlakte ')]/span/text()").get()
        if square_meters:
            item_loader.add_value('square_meters', square_meters.split("m")[0])
        room=response.xpath("//li[contains(.,'Slaapkamers ')]/span/text()").get()
        if room:
            item_loader.add_xpath('room_count', room)
        # available_date = "".join(response.xpath('.//*[contains(text(), "BESCHIKBAAR")]/../following::text()[1]').extract())
        # if "Beschikbaarheid :" in available_date and "vrij vanaf heden" not in available_date.lower():
        #     # available_date = available_date.split(":")[1].strip()
        #     available_date = re.findall(r"\d+\.\d+\.\d+", available_date)[0]
        #     available_date = datetime.strptime(available_date, '%d.%m.%Y').strftime("%d-%m-%Y")
        # if "vrij vanaf heden" in available_date.lower():
        #     available_date = date.today().strftime("%d-%m-%Y")
        # if available_date:
        #     item_loader.add_value('available_date', format_date(available_date, "%d-%m-%Y"))

        energy_label=response.xpath("//li[contains(.,'EPC ')]/span/text()").get()
        if energy_label:
            energy = energy_label.split("k")[0].strip()
            item_loader.add_value("energy_label",energy_label_calculate(int(float(energy.replace(",",".")))))
        parking=response.xpath("//li[contains(.,'Garage ')]/span/text()").get()
        if parking:
            if "Ja"==parking:
                item_loader.add_value("parking",True)
        terrace=response.xpath("//li[contains(.,'Garage ')]/span/text()").get()
        if terrace:
            if "Ja"==terrace:
                item_loader.add_value("terrace",True)

        description=response.xpath("//div[@class='description']//p//text()").get()
        if description:
            item_loader.add_value("description",description)

        latlng=response.xpath("//script[contains(.,'lng')]/text()").get()
        if latlng:
            latitude=latlng.split("addMarker")[-1].split(",")[0].split("lat")[-1].split("}")[0].replace('"',"").replace("\n","").replace(":","").strip()
            item_loader.add_value("latitude",latitude)
            longitude=latlng.split("addMarker")[-1].split(",")[-1].split("lng")[-1].split("}")[0].replace('"',"").replace("\n","").replace(":","").strip()
            item_loader.add_value("longitude",longitude)
        address=response.xpath("//span[@class='address']/a/text()").get()
        if address:
            item_loader.add_value('address', address)
        city=response.xpath("//span[@class='address']/a/text()").get()
        if city:
            item_loader.add_value('city', city.split(",")[-1].strip().split(" ")[-1])
        zipcode=response.xpath("//span[@class='address']/a/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(",")[-1].strip().split(" ")[0])

        item_loader.add_value('landlord_phone', '0484 84 84 86')
        item_loader.add_value('landlord_email', ' info@immo-zone.be')
        item_loader.add_value('landlord_name', 'Immo Zone')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", self.external_source)
        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
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
