# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces, remove_unicode_char
import re
from datetime import datetime
import lxml,js2xml

class Copragrim(scrapy.Spider): 
    name = "copragim_fr"
    allowed_domains = ["copragim.fr"]
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator=',' 
    scale_separator='.'
    position = 0

    def start_requests(self):
        
        start_urls = ['http://www.copragim.fr/recherche,incl_recherche_basic_ajax.htm?idpays=250&surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&idqfix=1&idtt=1&pres=basic&lang=fr&idtypebien=1%2C2&ANNLISTEpg=1']
        for url in start_urls:
            yield scrapy.Request(url = url,
                                callback = self.parse,
                                meta = {'request_url': url,'page':1})

    def parse(self, response, **kwargs):
        listings = response.xpath('//a[@class="recherche-annonces-lien"]/@href').extract()
        for url in listings:
            yield scrapy.Request(
                url = url,
                callback = self.get_property_details,
                meta = {'request_url' : url})

        if len(listings)>=15:
            next_page_url = response.meta.get('request_url')[:-1] + str(response.meta.get('page')+1)
            if next_page_url:
                yield scrapy.Request(
                url = url,
                callback = self.get_property_details,
                meta = {'request_url' : url,'page':response.meta.get('page')+1})

    def get_property_details(self, response):

        external_link = response.meta.get('request_url')

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Copragim_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', extract_number_only(response.xpath('//span[contains(text(),"Référence")]/text()').extract_first()))

        title = remove_white_spaces("".join(response.xpath('//h1[@itemprop="name"]//text()').extract()))
        if title:
            item_loader.add_value('title',title)
            address = title.split(' - ')[-1]
            item_loader.add_value('address',address)
            item_loader.add_value('zipcode',extract_number_only(address))
        city = response.xpath("//span[@itemprop='title']/text()").getall()
        if city:            
            item_loader.add_value("city", city[2])
        houses = ['terrace property',"house","student property","villa" , 'maison']
        apartments = ['flat', 'apartment', 'appartement']

        property_type = None
        if "studio" in title.lower():
            property_type = "studio"
        elif any(apartment in title.lower() for apartment in apartments):
            property_type = "apartment"
        elif any(house in title.lower() for house in houses):
            property_type = "house"
        item_loader.add_value('property_type', property_type)

        description = remove_white_spaces("".join(response.xpath('//p[@itemprop="description"]//text()').extract()))
        if description:
            item_loader.add_value('description',remove_unicode_char(description))
        
        item_loader.add_xpath('images','//div[@id="slider"]//img/@src')

        available_date = remove_white_spaces(response.xpath('//div[@class="bloc-detail-reference"][contains(text(),"Maj")]/text()').extract_first())
        if available_date:
            available_date = available_date.split('Maj : ')[-1]
            item_loader.add_value('available_date',format_date(available_date,date_format="%d/%m/%Y"))
        
        rent = extract_number_only(response.xpath('//span[@itemprop="price"]/text()').extract_first(),thousand_separator='.',scale_separator='.')
        if rent:
            item_loader.add_value('rent_string',str(rent) + '€')
        
        square_meters = extract_number_only(response.xpath('//li[@title="Surface"]//div[2]/text()').extract_first(),thousand_separator='.',scale_separator='.')
        if square_meters:
            item_loader.add_value('square_meters', square_meters)
            
        room_count = response.xpath("//li/div[contains(.,'Chambre')]/following-sibling::div/text()").get()
        if room_count and room_count.strip() != "0":
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//li/div[contains(.,'Pièces')]/following-sibling::div/text()").get()
            if room_count and room_count.strip() != "0":
                item_loader.add_value("room_count", room_count.strip())
            else:
                room_count1 = response.xpath("//li/div[contains(.,'Pièce')]/following-sibling::div/text()").get()
                if room_count1 and room_count1.strip() != "0":
                    item_loader.add_value("room_count", room_count1.strip())





        bathroom_count = response.xpath("//li/div[contains(.,'Salle')]/following-sibling::div/text()").get()
        if bathroom_count and bathroom_count.strip() != "0":
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        floor = extract_number_only(response.xpath('//li[@title="Etage"]/div[2]/text()').extract_first())
        if floor:
            item_loader.add_value('floor',floor)
        
        balcony = response.xpath('//li[@title="Balcon"]').extract_first()
        if balcony:
            item_loader.add_value('balcony',True)
        
        terrace = response.xpath('//li[@title="Terrasse"]').extract_first()
        if terrace:
            item_loader.add_value('terrace',True)

        elevator = response.xpath('//li[@title="Ascenseur"]').extract_first()
        if elevator:
            item_loader.add_value('elevator',True)
        
        item_loader.add_value('landlord_name','Copragim')
        item_loader.add_value("landlord_phone", "01 64 32 70 42")
            
        item_loader.add_value('landlord_email','contact@copragim.fr')

        self.position+=1
        item_loader.add_value('position',self.position)

        utilities = response.xpath("//li[contains(.,'Charges :')]/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].split(":")[1].replace(" ","")
            item_loader.add_value("utilities", utilities)
            
        deposit = response.xpath("//strong[contains(.,'Dépôt')]/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].split(":")[1].replace(" ","")
            item_loader.add_value("deposit", deposit)
        
        yield item_loader.load_item()