# -*- coding: utf-8 -*-
# Author: Karan Katle
# Team: Sabertooth 
import scrapy, copy, urllib
from ..loaders import ListingLoader
from python_spiders.helper import  remove_white_spaces, extract_number_only
import re
import lxml,js2xml
from parsel import Selector

class AgenceceiFrSpider(scrapy.Spider):
    name = "agence_cei_fr"
    allowed_domains = ["www.agence-cei.fr"]
    execution_type = 'testing'
    country = 'france'
    locale ='en'
    external_source='Agence_Cei_PySpider_france_en'
    thousand_separator=','
    scale_separator='.'
    api_url = 'http://www.agence-cei.fr/en/search/'
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
        listing = response.xpath('.//a[contains(text(),"Detailed view")]/@href').extract()
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
        
        bathroom_count = response.xpath('.//li[contains(text(), "bathroom")]/text()').extract_first()
        if bathroom_count and extract_number_only(bathroom_count)!='0':
            item_loader.add_value('bathroom_count',extract_number_only(bathroom_count))

        square_meters = response.xpath('.//li[contains(text(),"Surface")]//span/text()').extract_first()
        if square_meters:
            item_loader.add_xpath('square_meters',extract_number_only(square_meters,thousand_separator=',',scale_separator='.'))
            
        floor = response.xpath('.//li[contains(text(),"Floor")]//span/text()').extract_first()
        if floor:
            item_loader.add_value('floor',str(extract_number_only(floor.split()[0])))
            
        deposit = response.xpath('.//li[contains(text(),"Guarantee")]//span/text()').extract_first()
        if deposit:
            item_loader.add_value('deposit',extract_number_only(deposit,thousand_separator=',',scale_separator='.'))
        
        utilities = response.xpath('.//li[contains(text(),"Fees")]//span/text()').extract_first()
        if utilities:
            item_loader.add_value('utilities',extract_number_only(utilities,thousand_separator=',',scale_separator='.'))
            
        details1 = response.xpath('.//*[@class="services details"]//li/text()').extract()
        details2 = response.xpath('.//*[@class="areas details"]//li/text()').extract()
        if details1 and details2:
            detail = ', '.join(details1).lower()+', '+', '.join(details1).lower()
        elif details1:
            detail = ', '.join(details1).lower()
        elif details2:
            detail = ', '.join(details2).lower()

        if re.search(r'un[^\w]*furnish',detail):
            item_loader.add_value('furnished',False)
        elif re.search(r'not[^\w]*furnish',detail):
            item_loader.add_value('furnished',False)
        elif re.search(r'furnish',detail):
            item_loader.add_value('furnished',True)
        if re.search(r'dish[^\w]*washer',detail):
            item_loader.add_value('dishwasher',True)
        if re.search(r'swimming[^\w]*pool',detail) or 'pool' in detail:
            item_loader.add_value('swimming_pool',True)
        if 'terrace' in detail:
            item_loader.add_value('terrace',True)
        if 'balcony' in detail:
            item_loader.add_value('balcony',True)
        if 'parking' in detail:
            item_loader.add_value('parking',True)
        if 'lift' in detail or 'elevator' in detail:
            item_loader.add_value('elevator',True)
        if re.search(r'no[^\w]*pet\w{0,1}[^\w]*allow',detail):
            item_loader.add_value('pets_allowed',False)
        elif re.search(r'pet\w{0,1}[^\w]*not[^\w]*allow',detail):
            item_loader.add_value('pets_allowed',False)
        elif re.search(r'pet\w{0,1}[^\w]*allow',detail):
            item_loader.add_value('pets_allowed',True)
                
        room_count = response.xpath('.//li[contains(text(),"Rooms")]//span/text()').extract_first()
        if room_count and extract_number_only(room_count)!='0':
            item_loader.add_value('room_count',extract_number_only(room_count))
        
        item_loader.add_value("external_source", "Agence_Cei_PySpider_{}_{}".format(self.country, self.locale))
        
        javascript = response.xpath('.//script[contains(text(),"marker_map_2")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            lat_lng = selector.xpath('.//identifier[@name="marker_map_2"]/../..//number/@value').extract()
            if lat_lng and not any(i in lat_lng for i in ['0','0']):
                item_loader.add_value('latitude', lat_lng[0])
                item_loader.add_value('longitude', lat_lng[1])
                
        epc = response.xpath('.//img[@alt="Energy - Conventional consumption"]/@src').extract_first()
        if epc:
            ratingValue=epc.split('/')[-1]
            if ratingValue and ratingValue.isnumeric():
                ratingValue=int(ratingValue)
                if ratingValue>=451:
                    epc_class='G'
                elif ratingValue>=331 and ratingValue<=450:
                    epc_class='F'
                elif ratingValue>=231 and ratingValue<=330:
                    epc_class='E'
                elif ratingValue>=151 and ratingValue<=230:
                    epc_class='D'
                elif ratingValue>=91 and ratingValue<=150:
                    epc_class='C'
                elif ratingValue>=51 and ratingValue<=90:
                    epc_class='B'
                elif ratingValue>=1 and ratingValue<=50:
                    epc_class='A'
                item_loader.add_value('energy_label',epc_class)

        landlord_name = response.xpath('.//*[@class="profile"]/preceding-sibling::strong/text()').extract_first()
        if landlord_name:
            item_loader.add_value('landlord_name',remove_white_spaces(landlord_name))
        else:
            item_loader.add_value('landlord_name',"Centrale D'etudes Immobilieres")
        item_loader.add_xpath('landlord_phone','.//*[@class="userBlock"]//span[@class="phone smallIcon"]/a/text()')
        item_loader.add_xpath('landlord_email','.//*[@class="userBlock"]//span[@class="mail smallIcon"]/a/text()')

        address = response.xpath('.//meta[@name="keywords"]/@content').extract_first()
        if address and 'france' in address.lower():
            add_str = re.search(r'.*(?=France)',address,re.IGNORECASE)
            if add_str:
                address_str = remove_white_spaces(', '.join(add_str.group().split(',')[2:]))+' France'
                item_loader.add_value('address',address_str)
        elif address and 'room' in address.lower():
            add_str = re.search(r'.*(?=\d+\sroom)',address,re.IGNORECASE)
            if add_str:
                address_str = remove_white_spaces(', '.join(add_str.group().split(',')[2:])).strip(',')
                item_loader.add_value('address',address_str)
                
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()