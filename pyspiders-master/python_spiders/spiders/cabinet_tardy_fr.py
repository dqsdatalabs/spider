# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import remove_white_spaces, extract_rent_currency
import re 
import js2xml
import lxml.etree
from parsel import Selector


class CabinettardySpider(scrapy.Spider): 
    name = 'cabinet_tardy_fr'
    allowed_domains = ['cabinet-tardy.fr']
    start_urls = ['http://www.cabinet-tardy.fr']
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    thousand_separator = '.'
    scale_separator = ','
    position = 0
    external_source="CabinetTardy_PySpider_france_fr"
        
    def start_requests(self):
        start_urls = ['http://www.cabinet-tardy.fr/a-louer/1']
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})
            
    def parse(self, response, **kwargs):
        for property_url in response.xpath('.//*[@class="header"]//a/@href').extract():
            yield scrapy.Request(
                url='http://www.cabinet-tardy.fr'+property_url,
                callback=self.get_property_details,
                meta={'request_url': 'http://www.cabinet-tardy.fr'+property_url})
        
        if len(response.xpath('.//*[@class="header"]//a')) > 0:
            current_page = re.findall(r"(?<=louer/)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=louer/)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url}
            )
            
    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)

        item_loader.add_value('external_link', response.meta.get('request_url'))
        title = response.xpath('.//*[@class="detail-title"]/span/text()').extract_first()
        item_loader.add_xpath('title', './/*[@class="detail-title"]/span/text()')

        property_type = response.xpath('.//*[@class="breadcrumb-item active"]/text()').extract_first()
        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa']
        studio_types = ["studio"]
        
        if any(i in property_type.lower() for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        elif any(i in property_type.lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any(i in property_type.lower() for i in house_types):
            item_loader.add_value('property_type', 'house')
        else:
            return
        rent=response.xpath(".//th[contains(text(),'Loyer')]/following-sibling::th/text()").get()
        if rent:
            rent=rent.replace(" ","")
            item_loader.add_value("rent_string",rent)

        
        item_loader.add_xpath('deposit', './/th[contains(text(),"Dépôt de garantie")]/following-sibling::th/text()')
        item_loader.add_xpath('utilities', './/th[contains(text(),"Charges locatives")]/following-sibling::th/text()')
        item_loader.add_xpath('floor', './/th[contains(text(),"Etage")]/following-sibling::th/text()')

        # some listings have square meters in decimals
        square_meters = response.xpath('.//th[contains(text(),"Surface habitable")]/following-sibling::th/text()').extract_first()
        if square_meters:
            item_loader.add_value('square_meters', square_meters.split(',')[0])

        item_loader.add_xpath('description', './/p[@class="description"]/text()')
        item_loader.add_xpath('images', './/*[contains(@class,"imageGallery")]//img/@src')
        
        external_id = response.xpath('.//*[@class="labelprix ref"]/following-sibling::text()').extract_first()
        if external_id:
            item_loader.add_value('external_id', remove_white_spaces(external_id))
            
        room_count = response.xpath('.//th[contains(text(),"Nombre de chambre(s)")]/following-sibling::th/text()').extract_first()
        if (not room_count or room_count == '0') and any (i in property_type.lower() for i in studio_types):
            item_loader.add_value('room_count', '1')
        elif room_count:
            item_loader.add_value('room_count', room_count)
        else:
            room_count1=response.xpath('.//th[contains(text(),"Nombre de pièces")]/following-sibling::th/text()').extract_first()
            if room_count1:
               item_loader.add_value('room_count', room_count1)
                        
        bathroom_count = response.xpath('.//th[contains(text(),"Nb de salle d")]/following-sibling::th/text()').extract_first()
        if bathroom_count and bathroom_count != '0':
            item_loader.add_value('bathroom_count', bathroom_count)
            
        elevator = response.xpath('.//th[contains(text(),"Ascenseur")]/following-sibling::th/text()').extract_first()
        if elevator and elevator.lower() == "oui":
            item_loader.add_value('elevator', True)
        
        furnished = response.xpath('.//th[contains(text(),"Meublé")]/following-sibling::th/text()').extract_first()
        if furnished:
            if furnished.lower() == "oui":
                item_loader.add_value('furnished', True)
            else:
                item_loader.add_value('furnished', False)
        elif 'non meublé' in title.lower():
            item_loader.add_value('furnished', False)
        elif 'meublé' in title.lower():
            item_loader.add_value('furnished', True)
            
        balcony = response.xpath('.//th[contains(text(),"Balcon")]/following-sibling::th/text()').extract_first()
        if balcony and balcony.lower() == "oui":
            item_loader.add_value('balcony', True)
            
        terrace = response.xpath('.//th[contains(text(),"Terrasse")]/following-sibling::th/text()').extract_first()
        if terrace:
            if terrace.lower() == "oui":
                item_loader.add_value('terrace', True)
        elif 'terrasse' in title.lower():
            item_loader.add_value('terrace', True)
            
        # http://www.cabinet-tardy.fr/1009-f3-neuf-l-etrat.html
        parking = response.xpath('.//th[contains(text(),"Nombre de parking") or contains(text(),"Nombre de garage")]/following-sibling::th/text()').extract_first()
        if parking:
            if parking.lower() not in ['0', 'non']:
                item_loader.add_value('parking', True)
            else:
                item_loader.add_value('parking', False)
        elif 'parking' in title.lower():
            item_loader.add_value('parking', True)
            
        item_loader.add_value('landlord_name','Cbinet Tardy Immobilier')
        item_loader.add_value('landlord_phone','04.77.49.33.03')
       
            
        javascript = response.xpath('.//script[contains(text(),"lng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//property[@name="lat"]/number/@value').extract_first()
            longitude = selector.xpath('.//property[@name="lng"]/number/@value').extract_first()
            if latitude and longitude:
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)

        zipcode = response.xpath('.//th[contains(text(),"Code postal")]/following-sibling::th/text()').extract_first()
        if zipcode:
            item_loader.add_value('zipcode', remove_white_spaces(zipcode))
        
        city = response.xpath('.//th[contains(text(),"Ville")]/following-sibling::th/text()').extract_first()
        if city:
           item_loader.add_value('city', remove_white_spaces(city))
        city = item_loader.get_output_value('city')
        zipcode = item_loader.get_output_value('zipcode')
        if re.search(r'(-|–|—)',title):
            address = re.sub(('-|–|—'),'-',title).split('-')[1:]
            address = ' '.join([remove_white_spaces(i) for i in address if len(i)>1])
            item_loader.add_value('address',remove_white_spaces(address)+', '+city+', '+zipcode)
        else:
            address=title.split()[1:]
            address = ' '.join([remove_white_spaces(i) for i in address if len(i)>1])
            item_loader.add_value('address',remove_white_spaces(address)+', '+city+', '+zipcode)
            
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", self.external_source)
        yield item_loader.load_item()
