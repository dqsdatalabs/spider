# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy, copy, urllib
from ..loaders import ListingLoader
from python_spiders.helper import  remove_white_spaces, extract_number_only
import re
import lxml,js2xml
from parsel import Selector

class AgenceauberSpider(scrapy.Spider):
    name = "agenceauber_com"
    allowed_domains = ["www.agenceauber.com"]
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator=' '
    scale_separator='.'
    api_url = 'https://www.agenceauber.com/fr/recherche/'
    params = {  'nature': '2',
                'type[]': '',
                'price': '',
                'age': '',
                'tenant_min': '',
                'tenant_max': '',
                'rent_type': '',
                'newprogram_delivery_at': '',
                'newprogram_delivery_at_display': '',
                'currency': 'EUR',
                'customroute': '',
                'homepage': '' }
    #listing_new=[]
    position=0
    
    def start_requests(self):
        start_urls = [
                        {
                        'type[]': '1',
                         "property_type": "apartment"
                         },
                        {
                        'type[]': '2',
                         "property_type": "house"
                        },
                        {
                        'type[]': '6',
                         "property_type": "apartment"
                        }
                    ]
        for url in start_urls:
            params1 = copy.deepcopy(self.params)
            params1["type[]"] = url["type[]"]
            yield scrapy.Request(url=self.api_url + "?" + urllib.parse.urlencode(params1),
                                 callback=self.parse,
                                 meta={'request_url': self.api_url + "?" + urllib.parse.urlencode(params1),
                                       'params': params1,
                                       'property_type': url.get("property_type")})
                
    def parse(self, response, **kwargs):
        listing = response.xpath('.//a[contains(text(),"Vue détaillée")]/@href').extract()
        for property_url in listing:
            yield scrapy.Request(
                    url=response.urljoin(property_url),
                    callback=self.get_property_details,
                    meta={'request_url': response.urljoin(property_url),
                          "property_type": response.meta["property_type"]}
                )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_xpath('title','.//meta[@property="og:title"]/@content')
        item_loader.add_xpath('description','.//*[@id="description"]/text()')
        item_loader.add_xpath('images','.//a[@class="slideshow"]/@href')
        item_loader.add_xpath('rent_string','.//li[contains(text(),"€")]/text()')
        item_loader.add_value('external_link',response.meta.get('request_url'))
        item_loader.add_value('property_type',response.meta.get('property_type'))
        
        external_id = response.xpath('.//li[contains(text(),"Ref.")]/text()').extract_first()
        if external_id:
            item_loader.add_value('external_id',remove_white_spaces(external_id.split('.')[-1]))
            
        bathroom_count = response.xpath('.//li[contains(text(), "salle de bain")]/text()').extract_first()
        bathroom = response.xpath('.//li[contains(text(), "de douche")]/text()').extract_first()
        if bathroom_count and extract_number_only(bathroom_count)!='0':
            item_loader.add_value('bathroom_count',extract_number_only(bathroom_count))
        elif bathroom and extract_number_only(bathroom)!='0':
            item_loader.add_value('bathroom_count',extract_number_only(bathroom))

            
        square_meters = response.xpath('.//li[contains(text(),"Surface")]//span/text()').extract_first()
        if square_meters:
            item_loader.add_xpath('square_meters',extract_number_only(square_meters,thousand_separator=' ',scale_separator='.'))
            
        floor = response.xpath('.//li[contains(text(),"Etage")]//span/text()').extract_first()
        if floor:
            item_loader.add_value('floor',str(extract_number_only(floor.split()[0])))
            
        deposit = response.xpath('''.//li[contains(text(),"Honoraires")]//span/text()''').extract_first()
        if deposit:
            item_loader.add_value('deposit',extract_number_only(deposit,thousand_separator=' ',scale_separator='.'))
            
        details1 = response.xpath('.//*[@class="services details"]//li/text()').extract()
        details2 = response.xpath('.//*[@class="areas details"]//li/text()').extract()
        if details1 and details2:
            detail = ', '.join(details1).lower()+', '+', '.join(details1).lower()
        elif details1:
            detail = ', '.join(details1).lower()
        elif details2:
            detail = ', '.join(details2).lower()
            
        if re.search(r'non[^\w]*meublé',detail):
            item_loader.add_value('furnished',False)
        elif re.search(r'meublé',detail):
            item_loader.add_value('furnished',True)
            
        if re.search(r'ascenseur',detail) or re.search(r'lift',detail):
            item_loader.add_value('elevator',True)
            
        if re.search(r'terrasse',detail) or re.search(r'terrass',detail):
            item_loader.add_value('terrace',True)
                
        if re.search(r'lave[^\w]*vaisselle',detail):
            item_loader.add_value('dishwasher',True)
            
        if re.search(r'machine[^\w]*à[^\w]*laver',detail) or re.search('machine[^\w]*à[^\w]*laver[^\w]*le[^\w]*linge',detail):
            item_loader.add_value('washing_machine',True)
            
        if re.search(r'non[^\w]*balcon',detail):
            item_loader.add_value('balcony',False)
        elif re.search(r'balcon',detail):
            item_loader.add_value('balcony',True)
            
        if re.search(r'parking',detail):
            item_loader.add_value('parking',True)
            
        utilities = response.xpath('.//li[contains(text(),"Charge")]//span/text()').extract_first()
        if utilities:
            item_loader.add_value('utilities',extract_number_only(utilities,thousand_separator=' ',scale_separator='.'))

        city = "".join(response.xpath('//article/div/h2/text()').extract())
        if city:
            item_loader.add_value('city',city.strip().split(" ")[-1].strip())
            
        room_count = response.xpath('.//li[contains(text(),"Pièces")]//span/text()').extract_first()
        if room_count and extract_number_only(room_count)!='0':
            item_loader.add_value('room_count',extract_number_only(room_count))

        item_loader.add_value("external_source", "Agenceauber_PySpider_{}_{}".format(self.country, self.locale))
        
        javascript = response.xpath('.//script[contains(text(),"marker_map_2")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            lat_lng = selector.xpath('.//identifier[@name="marker_map_2"]/../..//number/@value').extract()
            if lat_lng and not any(i in lat_lng for i in ['0','0']):
                item_loader.add_value('latitude', lat_lng[0])
                item_loader.add_value('longitude', lat_lng[1])

        landlord_name = response.xpath('.//*[@class="profile"]/preceding-sibling::strong/text()').extract_first()
        if landlord_name:
            item_loader.add_value('landlord_name',remove_white_spaces(landlord_name))
        else:
            item_loader.add_value('landlord_name',"AGENCE AUBER")
        item_loader.add_xpath('landlord_phone','.//*[@class="userBlock"]//span[@class="phone smallIcon"]/a/text()')
        item_loader.add_xpath('landlord_email','.//*[@class="userBlock"]//span[@class="mail smallIcon"]/a/text()')
        
        address = response.xpath('.//meta[@name="keywords"]/@content').extract_first()
        if address and 'france' in address.lower():
            add_str = re.search(r'.*(?=France)',address,re.IGNORECASE)
            if add_str:
                address_str = remove_white_spaces(', '.join(add_str.group().split(',')[2:]))+' France'
                item_loader.add_value('address',address_str)
        elif address and 'pièces' in address.lower():
            add_str = re.search(r'.*(?=\d+\spièces)',address,re.IGNORECASE)
            if add_str:
                address_str = remove_white_spaces(', '.join(add_str.group().split(',')[2:])).strip(',')
                item_loader.add_value('address',address_str)
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()














            