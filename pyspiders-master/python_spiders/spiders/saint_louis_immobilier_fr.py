# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces, remove_unicode_char
import re
from datetime import datetime
import lxml,js2xml

class SaintLouisImmobilier(scrapy.Spider):
    name = "saintlouisimmobilier_fr"
    allowed_domains = ["saint-louis-immobilier.fr"]
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator=','
    scale_separator='.'
    position = 0

    def start_requests(self):
        start_urls = ["https://www.saint-louis-immobilier.fr/a-louer/habitation/1"]
        for url in start_urls:
            yield scrapy.Request(url = url,
                                callback = self.parse,
                                meta = {'request_url': url,'page':1})

    def parse(self, response, **kwargs):
        
        listings = response.xpath('//section[@class="listing2"]/ul/li//a/@href').extract()
        for url in listings:
            yield scrapy.Request(
                url = response.urljoin(url),
                callback = self.get_property_details,
                meta = {'request_url' : response.urljoin(url)})
        
        if len(listings)>=10:
            next_page_url = response.meta.get('request_url')[:-1] + str(response.meta.get('page') +1) 
            if next_page_url:
                yield scrapy.Request(
                        url=next_page_url,
                        callback=self.parse,
                        meta={'request_url':next_page_url,
                        'page':response.meta.get('page')+1})

    def get_property_details(self, response):

        external_link = response.meta.get('request_url')

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "SaintLouisImmobilier_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('external_link', external_link)

        ref = "".join(response.xpath("//span[@class='ref']/text()").extract())
        if ref:
            item_loader.add_value('external_id', ref.split(" ")[1].strip())

        item_loader.add_value('rent_string',"".join(remove_white_spaces("".join(response.xpath('//span[@class="label-caption"]/../text()').extract()).split(",")[0]).split(" ")))
        item_loader.add_value('currency', "EUR")
        
        title = remove_unicode_char(response.xpath('//div[@class="bienTitle"]/h2/text()').extract_first())
        if title:
            item_loader.add_value('title',remove_white_spaces(title))
            
            apartment_types = ["lejlighed", "appartement", "apartment", "piso", "flat", "atico",
                           "penthouse", "duplex", "dakappartement", "triplex"]
            house_types = ['hus', 'chalet', 'bungalow', 'maison', 'house', 'home', 'villa', 'huis', 'cottage', 'student property']
            studio_types = ["studio"]
            studio_type = response.xpath('//h1[@itemprop="name"]//text()[contains(.,"Studio")]').extract_first()
            for i in studio_types:
                if i in title.lower() or studio_type:
                    item_loader.add_value('property_type','studio')
            for i in house_types:
                if i in title.lower():
                    item_loader.add_value('property_type','house')
            for i in apartment_types:
                if i in title.lower():
                    item_loader.add_value('property_type','apartment')
            
            address = title.split(" - ")[-1].strip()
            item_loader.add_value('address',address)

        images = response.xpath('//ul[@class="imageGallery imageHC  loading"]//img/@src').extract()
        if images:
            images = [response.urljoin(i) for i in images]
        item_loader.add_value('images',images)
        description = remove_unicode_char(response.xpath('//p[@itemprop="description"]/text()').extract_first())
        item_loader.add_value('description',description)

        zipcode = response.xpath('//span[contains(text(),"Code postal")]/../span[@class="valueInfos "]/text()').extract_first()
        if zipcode:
            item_loader.add_value('zipcode',remove_white_spaces(zipcode))
        city = response.xpath('//span[contains(text(),"Ville")]/../span[@class="valueInfos "]/text()').extract_first()
        if city:
            item_loader.add_value('city',remove_white_spaces(city))
        square_meters = response.xpath('//span[contains(text(),"Surface habitable (m²)")]/../span[@class="valueInfos "]/text()').extract_first()
        if square_meters:
            item_loader.add_value('square_meters',square_meters.split("m")[0].split(",")[0])
        room_count = response.xpath('//span[contains(text(),"chambre")]/../span[@class="valueInfos "]/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count',extract_number_only(room_count))
        else:
            item_loader.add_xpath('room_count', '//span[contains(text(),"pièce")]/../span[@class="valueInfos "]/text()')

        bathroom_count = response.xpath('//div[@id="details"]//span[contains(text(),"Nb de salle d")]/../span[@class="valueInfos "]/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count',extract_number_only(bathroom_count))
        floor = response.xpath('//span[contains(text(),"Etage")]/../span[@class="valueInfos "]/text()').extract_first()
        if floor:
            item_loader.add_value('floor',extract_number_only(floor))
        elevator = response.xpath('//span[contains(text(),"Ascenseur")]/../span[@class="valueInfos "]/text()').extract_first()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value('elevator',True)
        parking = response.xpath('//span[contains(text(),"Nombre de garage")]/../span[@class="valueInfos "]/text()').extract_first()
        if parking:
            item_loader.add_value('parking',True)
        furnished = response.xpath('//span[contains(text(),"Meublé")]/../span[@class="valueInfos "]/text()').extract_first()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value('furnished',True)
            elif "non" in furnished.lower():
                item_loader.add_value('furnished',False)
        terrace = response.xpath('//span[contains(text(),"Terrasse")]/../span[@class="valueInfos "]/text()').extract_first()
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value('terrace',True)
            elif "non" in terrace.lower():
                item_loader.add_value('terrace',False)
        javascript = response.xpath('//script[contains(text(),"getMap")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = scrapy.Selector(text=xml)
            item_loader.add_value('latitude',xml_selector.xpath('//property[@name="lat"]/number/@value').extract_first())
            item_loader.add_value('longitude',xml_selector.xpath('//property[@name="lng"]/number/@value').extract_first())
        
        deposit = response.xpath("//span[contains(.,'Dépôt')]/following-sibling::span/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip().split(",")[0].replace(" ","")
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//span[contains(.,'Charge')]/following-sibling::span/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        item_loader.add_value('landlord_name', 'Saint Louis Immobilier')
        item_loader.add_xpath('landlord_phone', '//a[contains(@href,"tel")]/text()')
        item_loader.add_xpath('landlord_email', '//a[contains(@href,"mailto")]/text()')
        
        self.position += 1
        item_loader.add_value('position', self.position)

        yield item_loader.load_item()

