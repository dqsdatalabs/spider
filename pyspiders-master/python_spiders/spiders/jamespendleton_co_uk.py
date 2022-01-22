# Author: Madhumitha S
# Team: Sabertooth
import scrapy
from ..loaders import ListingLoader
from ..helper import remove_white_spaces, convert_string_to_numeric, extract_number_only
import json
import re



class JamespendletonCoUkSpider(scrapy.Spider):
    
    name = "jamespendleton_co_uk"
    allowed_domains = ["jamespendleton.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    
    def start_requests(self):
        
        start_urls = [
            {'url' : 'https://www.jamespendleton.co.uk/wp-json/wp/v2/properties?_embed&deal_type=to-let&status=available&orderby=price&order=desc&page=1&per_page=16',
            'start_position' : 0,
            'page' : 1}
        ]
        for url in start_urls:
            yield scrapy.Request(url.get('url'),
                            callback= self.parse,
                            dont_filter=True,
                            method="GET",
                            headers={'Content-Type': 'application/json; charset=UTF-8'},
                            meta = {'url' : url.get('url'),
                                'start_position' : url.get('start_position'),
                                'page' : url.get('page')})

    def parse(self, response, **kwargs):
        url = response.meta.get('url')
        temp_json = json.loads(response.body)
        total = temp_json["total"]
        page = response.meta.get('page')
        start_position = response.meta.get('start_position')
        items = temp_json['items']
        for item in items:
            yield scrapy.Request(f"https://www.jamespendleton.co.uk/wp-json/wp/v2/properties/{item['id']}",
                            callback=self.parse_map,
                            dont_filter=True,
                            method="GET",
                            headers={'Content-Type': 'application/json; charset=UTF-8'},
                            meta={'item' : item})
        if total>16:
            if start_position <= total:
                start_position+=16
                yield scrapy.Request(url.split('&page=1')[0] + f'&page={page+1}' + url.split('&page=1')[1],
                            callback= self.parse,
                            dont_filter=True,
                            method="GET",
                            headers={'Content-Type': 'application/json; charset=UTF-8'},
                            meta = {'url' : url,
                                    'start_position' : start_position,
                                    'page' : page+1})

    def parse_map(self, response):
        lat_lng = json.loads(response.body)
        item = response.meta.get('item')
        yield scrapy.Request(item['url'],
                            callback=self.get_property_details,
                            meta={'external_id' : item['id'],
                            'external_link' : item['url'],
                            'title' : item['title'],
                            'bedroom_count' : item['bedrooms'],
                            'price' : item['price'],
                            'address' : item['address'],
                            'latitude' : lat_lng['property']['lat'],
                            'longitude' : lat_lng['property']['lng']})        

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('external_link'))
        item_loader.add_value('external_id', response.meta.get('external_id'))
        item_loader.add_value('title', response.meta.get('title'))
        item_loader.add_value('room_count', response.meta.get('bedroom_count'))
        item_loader.add_value('bathroom_count', remove_white_spaces(response.xpath('//li[contains(@class,"baths")]/text()').extract_first()))        
        rent_string = response.meta.get('price')
        rent = re.findall(r'(£\d+,?\d+) pcm', rent_string)
        if rent:
            item_loader.add_value('rent_string', rent[0])
        item_loader.add_xpath('images', '//div[contains(@class,"properties-detail__gallery")]//@src')
        floor_plan_images = response.xpath('(.//div[contains(@class,"tabs__content")])[3]//@src').extract_first()
        if floor_plan_images:
            item_loader.add_value('floor_plan_images', floor_plan_images)            
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('address', response.meta.get('address'))        
        item_loader.add_value('city', 'London')
        zipcode = re.findall(r'\w+\d+',item_loader.get_output_value('address'))
        if zipcode:
            item_loader.add_value('zipcode', zipcode[0])
        item_loader.add_value('latitude', response.meta.get('latitude'))
        item_loader.add_value('longitude', response.meta.get('longitude'))
        item_loader.add_xpath('description', '//div[contains(@class,"overview__main")]/p/text()')
        square_feet = response.xpath('//li[contains(text(),"sq/ft")]/text()').extract_first()
        if square_feet:
            square_num = str(int(float(extract_number_only(square_feet,thousand_separator=',',scale_separator='.'))))
            square_meters = convert_string_to_numeric(square_num,JamespendletonCoUkSpider)*0.092903
            item_loader.add_value('square_meters', square_meters)
        features = response.xpath('//div[contains(@class,"overview__main")]/ul//li/text()').extract()
        features = " ".join([remove_white_spaces(f) for f in features ])
        property_type = response.xpath('.//ul[@class="properties-detail__list"]/li[3]/text()').extract_first()    
        apartment_types = ["apartment", "flat", "penthouse", "duplex", "dakappartement", "triplex"]
        house_types = ['bungalow', 'maison', 'house', 'home', 'house detached', 'detached', 'house terrace', 'terrace', 'maisonette']
        studio_types = ["studio", 'studio flat']

        if any (i in property_type.lower() or i in item_loader.get_output_value('description') for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        elif any (i in property_type.lower() or i in item_loader.get_output_value('description') for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any (i in property_type.lower() or i in item_loader.get_output_value('description') for i in house_types):
            item_loader.add_value('property_type', 'house')

        # https://www.jamespendleton.co.uk/property/all-saints-court-sw11-jamrps-bnl200438-2/
        if "parking" in features.lower():
            item_loader.add_value('parking', True)

        # https://www.jamespendleton.co.uk/property/battersea-park-view-sw8-jamrps-bnl200273-2/    
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        # https://www.jamespendleton.co.uk/property/eastfields-avenue-sw18-jamrps-bnl200377/
        if "swimming pool" in features.lower():
            item_loader.add_value('swimming_pool', True)

        # https://www.jamespendleton.co.uk/property/all-saints-court-sw11-jamrps-bnl200438-2/    
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        # https://www.jamespendleton.co.uk/property/sulivan-court-sw6-jamrps-css200796/    
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)
            
        if " furnished" in features.lower():
            item_loader.add_value('furnished', True)

        # https://www.jamespendleton.co.uk/property/sulivan-court-sw6-jamrps-css200796/
        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)

        #https://www.jamespendleton.co.uk/property/sulivan-court-sw6-jamrps-css200796/
        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)
        
        if response.xpath('.//a[contains(@href,"tel")]/text()'):    
            item_loader.add_xpath('landlord_phone', './/a[contains(@href,"tel")]/text()')
        if not response.xpath('.//a[contains(@href,"tel")]/text()'):
            item_loader.add_value('landlord_phone', '02076271111')
        if response.xpath('.//a[contains(@href,"mailto")]/text()'):
            item_loader.add_xpath('landlord_email', './/a[contains(@href,"mailto")]/text()')
        if not response.xpath('.//a[contains(@href,"mailto")]/text()'):
            item_loader.add_value('landlord_email', 'hello@jphomes.co.uk')
        item_loader.add_value('landlord_name', 'James Pendleton')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Jamespendleton_PySpider_{}_{}".format(self.country, self.locale))

        yield item_loader.load_item()