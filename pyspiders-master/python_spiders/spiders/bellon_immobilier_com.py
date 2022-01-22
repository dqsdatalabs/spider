# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
import js2xml
import lxml.etree
import scrapy
from scrapy import Selector
from ..helper import extract_number_only, extract_rent_currency, remove_white_spaces,format_date
from ..loaders import ListingLoader
from math import ceil
import dateparser
class BellonImmobilierComSpider(scrapy.Spider):
    name = 'bellon_immobilier_com'
    allowed_domains = ['bellon-immobilier.com']
    start_urls = ['http://bellon-immobilier.com']
    execution_type = 'testing'
    country = 'france' 
    locale = 'fr'
    thousand_separator=' '
    scale_separator=','
    position = 0
    custom_settings = {'HTTPCACHE_ENABLED': False}
    external_source="Bellon_Immobilier_PySpider_france_fr"
    def start_requests(self):     
        start_urls = ["http://bellon-immobilier.com/location.php"]   
        for url in start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                )

    def parse(self, response, **kwargs):
        page = response.meta.get("page", 2)
        seen = False
        for external_link in response.xpath("//a[contains(@class,'boutons') and contains(., 'CONSULTER')]/@href").extract():#//div[@class='biens']//div[@class='demi']/a/@href
            yield scrapy.Request(
                url = response.urljoin(external_link),
                callback=self.get_property_details,
                meta={
                    'external_link' : external_link,
                    })
            seen = True
 
        if page == 2 or seen:
            follow_url = f"https://bellon-immobilier.com/pagelocation-BellonImmobilier-{page}.html"
            yield scrapy.Request(
                url = follow_url,
                callback=self.parse,
                meta={"page":page+1})

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        item_loader.add_value("external_source","Bellon_Immobilier_PySpider_france_fr")
        external_id = response.xpath("//p[@class='titres_orange']/text()[contains(.,'Référence :')]").extract_first()
        if external_id:
            item_loader.add_value('external_id', external_id.split(":")[-1].strip())
        images = response.xpath("//div[@class='slider slider-for']//img/@src").extract()
        item_loader.add_value('images', [f"http://bellon-immobilier.com/{img}" for img in images])
        item_loader.add_xpath('description', "//p[@class='titres_orange']/following-sibling::p//text()")
        item_loader.add_value("currency", "EUR")
        square_meters = response.xpath("//td[img[contains(@src,'surface')]]/following-sibling::td[1]/text()").extract_first()
        if square_meters:
            square_meters = str(int(ceil(float(extract_number_only(remove_white_spaces(square_meters),thousand_separator=' ',scale_separator=',')))))
            item_loader.add_value('square_meters', square_meters)
  
        item_loader.add_xpath('title', "//p[@class='gros_marron']/text()")
        item_loader.add_xpath('room_count', "//td[img[@alt='Pièces']]/following-sibling::td[1]/text()")
        
        rent = response.xpath("//p[@class='gros_orange']/text()[contains(.,'€')]").extract_first()
        if rent:
            item_loader.add_value('rent', str(rent.replace('€', '').strip()))
    
        address1 = response.xpath("//p[@class='gros_marron']/text()").get()
        address2 = response.xpath("//p[@class='titres_orange']/following-sibling::p//text()").get()
        if 'senlis' in address1.lower() or 'senlis' in address2.lower():
            item_loader.add_value('address', "SENLIS")
            item_loader.add_value('city', "SENLIS")
        elif ('chantilly' in address1.lower()) or ('chantilly' in address2.lower()):
            item_loader.add_value('address', "CHANTILLY")
            item_loader.add_value('city', "CHANTILLY")
        try:    
            available_date = response.xpath("//title[contains(., 'LIBRE') or contains(.,'Libre')]/text()").get()
            if available_date:
                date_parsed = dateparser.parse(available_date.split(".")[0].lower().replace("libre", "").replace('le',"").strip())
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
            elif response.xpath("//p[contains(., 'Disponible') or contains(., 'DISPONIBLE')]/text()").get():
                available_date = response.xpath("//p[contains(., 'Disponible')]/text()").re_first(r'Disponible.*(\d{2}.\d{2}.\d{4})') or  \
                                    response.xpath("//p[contains(., 'Disponible')]/text()").re_first(r'Disponible.*(\d{2}.*\d{4})') or \
                                    response.xpath("//p[contains(., 'DISPONIBLE')]/text()").re_first(r'DISPONIBLE.*(\d+.*\d{4})')
                date_parsed = dateparser.parse(available_date)
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        except:
            pass 
        utility = response.xpath("//p[contains(.,'charges')]/text()").re_first(r'(\d+) EUR de charges')
        if utility:
            item_loader.add_value('utilities', utility)
        property_string = response.xpath("//p[@class='gros_marron']/text()").extract_first()
        if property_string:
            property_string = remove_white_spaces(property_string)
            apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
            house_types = ['mobile home','park home','character property',
                'chalet', 'bungalow', 'maison', 'house', 'home', ' villa ',
                'holiday complex', 'cottage', 'semi-detached']
            studio_types = ["studio"]
            if any (i in property_string.lower() for i in studio_types):
                item_loader.add_value('property_type','studio')
            elif any (i in property_string.lower() for i in apartment_types):
                item_loader.add_value('property_type','apartment')
            elif any (i in property_string.lower() for i in house_types):
                item_loader.add_value('property_type','house')
            else: return
            
        item_loader.add_value('landlord_name','Bellon Immobilier')
        item_loader.add_value('landlord_phone', '03 44 24 19 00')
        item_loader.add_value('landlord_email', 'contact@bellon-immobilier.com')
        self.position+=1
        item_loader.add_value('position',self.position)
        
        print(response.url)
        yield item_loader.load_item()