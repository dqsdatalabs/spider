# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces, remove_unicode_char
import re
from datetime import datetime
import lxml,js2xml

class RelocationGuyane(scrapy.Spider): 
    name = "relocationguyane_com" 
    allowed_domains = ["relocationguyane.com"]
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    external_source='RelocationGuyane_PySpider_france_fr'
    thousand_separator=','
    scale_separator='.'
    position = 0

    def start_requests(self): 
        
        start_urls = ['http://www.relocationguyane.com/a-louer/1']
        for url in start_urls:
            yield scrapy.Request(url = url,
                                callback = self.parse,
                                meta = {'request_url': url,'page':1})

    def parse(self, response, **kwargs):
        
        listings = response.xpath("//ul[@class='listingUL']//li//h1[@itemprop='name']/a/@href").extract()
        for url in listings:
            yield scrapy.Request(
                url = response.urljoin(url),
                callback = self.get_property_details,
                meta = {'request_url' : response.urljoin(url)})

        next_page_url = response.xpath('//span[contains(text(),"»")]/../@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                    url=response.urljoin(next_page_url),
                    callback=self.parse,
                    meta={
                    'request_url':response.urljoin(next_page_url)})

    def get_property_details(self, response):

        external_link = response.meta.get('request_url')

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "RelocationGuyane_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('external_link', external_link)
        external_id = response.xpath('//span[@class="ref"]/text()').extract_first()
        if external_id:
            item_loader.add_value('external_id', extract_number_only(external_id))
        
        rent = response.xpath('//span[@class="prix"]/text()').extract_first()
        if rent:
            rent = "".join(remove_white_spaces(rent).split(' '))
            item_loader.add_value('rent_string',rent)
        item_loader.add_value('currency',"EUR")
        deposit = response.xpath('//p[span[.="Dépôt de garantie TTC"]]/span[2]/text()').extract_first()
        if deposit:
            deposit = "".join(remove_white_spaces(deposit).split(' '))
            item_loader.add_value('deposit',deposit)
        utilities = response.xpath('//p[span[contains(.,"Charges locatives")]]/span[2]/text()').extract_first()
        if utilities:
            utilities = "".join(remove_white_spaces(utilities).split(' '))
            item_loader.add_value('utilities',utilities)
        title = response.xpath('//div[@class="col-lg-6 col-md-6 col-sm-12 diapoDetail"]//h1/text()').extract_first()
        if title:
            item_loader.add_value('title',remove_white_spaces(title))
        
        houses = ['terrace property',"house","student property","villa" , 'maison']
        apartments = ['flat', 'apartment', 'appartement']

        property_type = None
        if title and "studio" in title.lower():
            property_type = "studio"
        elif title and any(apartment in title.lower() for apartment in apartments):
            property_type = "apartment"
        elif title and any(house in title.lower() for house in houses):
            property_type = "house"
        elif title and "duplex" in title.lower():
            property_type = "house"
        item_loader.add_value('property_type', property_type)

        description = response.xpath('//p[@itemprop="description"]/text()').extract_first()
        if description:
            item_loader.add_value('description',description)
            
        images = [response.urljoin(x) for x in response.xpath('//div[@class="carousel-inner"]//li//img/@src').extract()]
        if images:
            item_loader.add_value("images", images)
        zipcode = response.xpath('//div[@id="infos"]//span[contains(text(),"Code postal")]/..//span[@class="valueInfos "]/text()').extract_first()
        if zipcode:
            item_loader.add_value('zipcode',remove_white_spaces(zipcode))
        city = response.xpath('//div[@id="infos"]//span[contains(text(),"Ville")]/..//span[@class="valueInfos "]/text()').extract_first()
        if city:
            item_loader.add_value('city',remove_white_spaces(city))
            address = remove_white_spaces(city)+", "+remove_white_spaces(zipcode)
            item_loader.add_value('address', address)     

        square_meters = response.xpath('//div[@id="infos"]//span[contains(text(),"Surface habitable (m²)")]/..//span[@class="valueInfos "]/text()').extract_first()
        if square_meters:    
            item_loader.add_value('square_meters',extract_number_only(square_meters))
        item_loader.add_xpath('room_count','//div[@id="infos"]//span[contains(text(),"Nombre de pièces")]/..//span[@class="valueInfos "]/text()')
        item_loader.add_xpath('bathroom_count','//div[@id="details"]//span[contains(text(),"Nb de salle d")]/..//span[@class="valueInfos "]/text()')
        item_loader.add_xpath('floor','//div[@id="infos"]//span[contains(text(),"Etage")]/..//span[@class="valueInfos "]/text()')
        furnished = response.xpath('//div[@id="infos"]//span[contains(text(),"Meublé")]/..//span[@class="valueInfos "]/text()').extract_first()
        if furnished:    
            if "oui" in furnished.lower():
                item_loader.add_value('furnished',True)
            elif "non" in furnished.lower():
                item_loader.add_value('furnished',False)
        terrace = response.xpath('//div[@id="details"]//span[contains(text(),"Terrasse")]/..//span[@class="valueInfos "]/text()').extract_first()
        if terrace:    
            if "oui" in terrace.lower():
                item_loader.add_value('terrace',True)
            elif "non" in terrace.lower():
                item_loader.add_value('terrace',False)
        elevator = response.xpath('//div[@id="infos"]//span[contains(text(),"Ascenseur")]/..//span[@class="valueInfos "]/text()').extract_first()
        if elevator:    
            if "oui" in elevator.lower():
                item_loader.add_value('elevator',True)
            elif "non" in elevator.lower():
                item_loader.add_value('elevator',False)
        balcony = response.xpath('//div[@id="details"]//span[contains(text(),"Balcon")]/..//span[@class="valueInfos "]/text()').extract_first()
        if balcony:    
            if "oui" in balcony.lower():
                item_loader.add_value('balcony',True)
            elif "non" in balcony.lower():
                item_loader.add_value('balcony',False)

        item_loader.add_value('landlord_name','Guyane Immobiliers')
        item_loader.add_xpath('landlord_phone','//li[@class="phone"]//a/text()')
        item_loader.add_xpath('landlord_email','//li[@class="email"]//a/text()')
        lat_lng = response.xpath("//script[contains(.,'center: { lat :')]//text()").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split("center: { lat :")[-1].split(",")[0].strip())
            item_loader.add_value("longitude", lat_lng.split("center: { lat :")[-1].split("lng:")[1].split("}")[0].strip())

        self.position+=1
        item_loader.add_value('position',self.position)

        yield item_loader.load_item()


        

