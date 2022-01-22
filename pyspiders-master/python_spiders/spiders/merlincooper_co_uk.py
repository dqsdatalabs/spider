# -*- coding: utf-8 -*-
# Author: Karan Katle
# Team: Sabertooth 
import scrapy, copy, urllib
from ..loaders import ListingLoader
from python_spiders.helper import extract_number_only, remove_white_spaces, convert_string_to_numeric
import re
import math

class MerlincooperSpider(scrapy.Spider):
    name = "merlincooper_co_uk"
    allowed_domains = ["merlincooper.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    listing_new =[]
    api_url = 'http://www.merlincooper.co.uk/'
    params = {  'id': 43839,
                'do': 'search',
                'for': 2,
                'type[]': '',
                'minprice': 0,
                'maxprice': 99999999999,
                'kwa[]': '',
                'minbeds': 0,
                'id': 43839,
                'order': 2,
                'page': 0,
                'do': 'search' }
    position=0

    def start_requests(self):
        start_urls = [
                        {'type[]': 6,
                         "property_type": "house"
                         },
                        {
                        'type[]': 7,
                         "property_type": "apartment"
                        },
                        {
                        'type[]': 8,
                         "property_type": "apartment"
                        },
                        {
                        'type[]': 9,
                         "property_type": "house"
                        },
                        {
                        'type[]': 10,
                         "property_type": "house"
                        },
                        {
                        'type[]': 11,
                         "property_type": "house"
                        },
                        # Warehouse
                        # {
                        # 'type[]': 12,
                        #  "property_type": ""
                        # },
                        # Short let
                        # {
                        # 'type[]': 13,
                        #  "property_type": ""
                        # }
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
        listing = response.xpath('.//a[@class="results-link"]/@href').extract()
        for property_url in listing:
            if property_url not in self.listing_new:
                self.listing_new.append(property_url)

                yield scrapy.Request(
                    url=response.urljoin(property_url),
                    callback=self.get_property_details,
                    meta={'request_url': response.urljoin(property_url),
                        "property_type": response.meta["property_type"]}
                )

        if len(response.xpath('.//a[@class="results-link"]')) > 0:
            current_page = response.meta["params"]["page"]
            params1 = copy.deepcopy(response.meta["params"])
            params1["page"] = current_page + 1
            next_page_url = self.api_url + "?" + urllib.parse.urlencode(params1)
            yield scrapy.Request(
                url=self.api_url + "?" + urllib.parse.urlencode(params1),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'params': params1,
                      "property_type": response.meta["property_type"]}
            )
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        external_link= response.meta.get('request_url')
        item_loader.add_value('external_link',external_link)
        external_id=re.search(r'(?<=pid=)\d+',external_link).group()
        item_loader.add_value('external_id',external_id)
        
        item_loader.add_xpath('description','.//*[@class="details-description"]/p/text()')

        # apartment_types = ["appartement", "apartment", "flat",
        #                    "penthouse", "duplex", "triplex", "residential complex"]
        # house_types = ['chalet', 'bungalow', 'maison', 'house', 'home',"villa","maisonette"]
        # studio_types = ["studio","studio apartment","studio flat"]
        property_type= response.meta.get('property_type')
        if len(property_type)>0:
            item_loader.add_value('property_type',property_type)
        # elif len(property_type) == 0:
        #     property_type = response.xpath('.//*[@class="icon-house"]/following-sibling::text()').extract_first()
        #     if property_type:
        #         if property_type.lower() in studio_types:
        #             item_loader.add_value('property_type','studio')
        #         if property_type.lower() in house_types:
        #             item_loader.add_value('property_type','house')
        #         if property_type.lower() in apartment_types:
        #             item_loader.add_value('property_type','apartment')
        #     elif header1:
        #         if any (word in header1.lower() for word in apartment_types + house_types + studio_types):
        #             if any (i in header1.lower() for i in studio_types):
        #                 item_loader.add_value('property_type','studio')
        #             elif any (i in header1.lower() for i in house_types):
        #                 item_loader.add_value('property_type','house')
        #             elif any (i in header1.lower() for i in apartment_types):
        #                 item_loader.add_value('property_type','apartment')
        #         elif any (word in description.lower() for word in apartment_types + house_types + studio_types):
        #             if any (i in description.lower() for i in studio_types):
        #                 item_loader.add_value('property_type','studio')
        #             elif any (i in description.lower() for i in house_types):
        #                 item_loader.add_value('property_type','house')
        #             elif any (i in description.lower() for i in apartment_types):
        #                 item_loader.add_value('property_type','apartment')
        
        room_count = response.xpath('.//*[@class="icon-bed"]/following-sibling::text()').extract_first()
        if room_count:
            rooms = str(extract_number_only(room_count,thousand_separator=',',scale_separator='.'))
            if rooms != '0' and rooms.isnumeric():
                item_loader.add_value('room_count',rooms)
            elif (rooms == '0' or rooms.isnumeric()==False) and item_loader.get_output_value('property_type')=='studio':
                    item_loader.add_value('room_count','1')
        elif room_count == None and item_loader.get_output_value('property_type')=='studio':
            item_loader.add_value('room_count','1')

        bathroom_count = response.xpath('.//*[@class="icon-bath"]/following-sibling::text()').extract_first()
        if bathroom_count:
            bathrooms = str(extract_number_only(bathroom_count,thousand_separator=',',scale_separator='.'))
            if bathrooms != '0' and bathrooms.isnumeric():
                item_loader.add_value('bathroom_count',bathrooms)

        square_meters = response.xpath('.//*[@class="icon-size"]/following-sibling::text()').extract_first()
        if square_meters:
            square = str(int(float(extract_number_only(square_meters,thousand_separator=',',scale_separator='.'))))
            if square != '0' and 'sqm' in square_meters:
                item_loader.add_value('square_meters',extract_number_only(square_meters,thousand_separator=',',scale_separator='.'))
            elif square !='0' and 'sq ft' in square_meters:
                square_upd = convert_string_to_numeric(square_meters,MerlincooperSpider)*0.092903
                item_loader.add_value('square_meters',str(math.ceil(square_upd)))

        rent_string = response.xpath('.//*[@class="detail-price"]/text()').extract_first()
        if rent_string and 'pw' in rent_string.lower():
            rent = convert_string_to_numeric(rent_string,MerlincooperSpider)*4
            item_loader.add_value('rent_string','£'+str(math.ceil(rent)))
        elif rent_string:
            rent = convert_string_to_numeric(rent_string,MerlincooperSpider)
            item_loader.add_value('rent_string','£'+str(math.ceil(rent)))

        # http://www.merlincooper.co.uk/search~action=detail,pid=5914
        parking = response.xpath('.//*[@class="icon-parking"]/following-sibling::text()').extract_first()
        if 'yes' in parking.lower():
            item_loader.add_value('parking',True)
        elif 'no' in parking.lower():
            item_loader.add_value('parking',False)

        pets = response.xpath('.//*[@class="icon-pet"]/following-sibling::text()').extract_first()
        if 'yes' in pets.lower():
            item_loader.add_value('pets_allowed',True)
        elif 'no' in pets.lower():
            item_loader.add_value('pets_allowed',False)

        item_loader.add_xpath('images','.//*[@id="photos"]//img/@src')
        item_loader.add_xpath('floor_plan_images','.//*[@id="floorplan"]//img/@src')
        title = response.xpath('.//title[contains(text(),"Merlin Cooper")]/text()').extract_first()
        item_loader.add_value('title',remove_white_spaces(title.split(' - ')[0]))
        features = response.xpath('.//*[@class="details-features"]/text()').extract_first()
        if features:
            if 'unfurnished' in features.lower() and ' furnished' not in features.lower():
                item_loader.add_value('furnished',False)
            # http://www.merlincooper.co.uk/search~action=detail,pid=2063
            elif ' furnished' in features.lower() and 'unfurnished' not in features.lower():
                item_loader.add_value('furnished',True)
            if 'terrace' in features.lower():
                item_loader.add_value('terrace',True)
            # http://www.merlincooper.co.uk/search~action=detail,pid=130
            if 'balcony' in features.lower():
                item_loader.add_value('balcony',True)
            if 'dishwasher' in features.lower():
                item_loader.add_value('dishwasher',True)
            if 'washing machine' in features.lower():
                item_loader.add_value('washing_machine',True)
            if 'swimming pool' in features.lower():
                item_loader.add_value('swimming_pool',True)

        address = response.xpath('.//h3[@class="details-address1"]/text()').extract_first()
        if address:
            address_new = address.replace('|',',')
            item_loader.add_value('address', address_new)
            address_list = address.split('|')
            if len(address_list)==2:
                if remove_white_spaces(address_list[-1]).isnumeric() or remove_white_spaces(address_list[-1]).isalnum():
                    item_loader.add_value('zipcode',remove_white_spaces(address_list[-1]))
                item_loader.add_value('city', remove_white_spaces(address_list[0]))
            elif len(address_list)==1:
                if remove_white_spaces(address_list[-1]).isnumeric() or remove_white_spaces(address_list[-1]).isalnum():
                    item_loader.add_value('zipcode',remove_white_spaces(address_list[0]))
                else:
                    item_loader.add_value('city', remove_white_spaces(address_list[0]))
            
        latlng = response.xpath('.//iframe[contains(@src,"maps")]/@src').extract_first()
        if latlng:
            latlng = re.search(r'(?<=cbll=).+?(?=&cbp=)',latlng).group().split(',')
            item_loader.add_value('latitude', latlng[0])
            item_loader.add_value('longitude', latlng[1])

        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.split('_')[0].capitalize(), self.country, self.locale))
        item_loader.add_value('landlord_name','Merlin Cooper')
        if "coventry" in address.lower() or 'coventry' in title.lower():
            item_loader.add_value('landlord_phone','024 7697 0065')
            item_loader.add_value('landlord_email','coventry@merlincooper.co.uk')
        elif "surrey" in address.lower() or "surrey" in title.lower():
            item_loader.add_value('landlord_phone','01483 943518')
            item_loader.add_value('landlord_email','surrey@merlincooper.co.uk')     
        else:
            item_loader.add_value('landlord_phone','020 3553 1067')
            item_loader.add_value('landlord_email','contact@merlincooper.co.uk')         
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
                