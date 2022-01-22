# -*- coding: utf-8 -*- 
# Author: Mehmet Kurtipek
import scrapy 
from ..loaders import ListingLoader
from python_spiders.helper import  remove_white_spaces, extract_rent_currency, format_date, remove_unicode_char
import re 
 
class InfinitipropertiesSpider(scrapy.Spider):
    name = "infinitiproperties_com"
    allowed_domains = ["infinitiproperties.com"]
    execution_type = 'testing'
    country = 'united_kingdom' 
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    position=0

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.infinitiproperties.com/property-search-results/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=39',
                'property_type': 'studio'},
            {'url': 'https://www.infinitiproperties.com/property-search-results/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=31',
                'property_type': 'house'},
            {'url': 'https://www.infinitiproperties.com/property-search-results/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=35',
                'property_type': 'apartment'},
            {'url': 'https://www.infinitiproperties.com/property-search-results/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=22',
                'property_type': 'house'},
                    ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                          callback=self.parse,
                          meta={'request_url': url.get('url'),
                                'property_type': url.get('property_type')}
                          )
                
    def parse(self, response, **kwargs):
        listing = response.xpath('.//*[@class="jd-button"]/@href').extract()
        for property_url in listing:
            yield scrapy.Request(
                url=response.urljoin(property_url),
                callback=self.get_property_details,
                meta={'request_url': response.urljoin(property_url),
                      "property_type": response.meta["property_type"]}
            )

        if len(response.xpath('.//*[@class="jd-button"]')) > 0:
            next_page_url = response.xpath('.//*[@class="next page-numbers"]/@href').extract_first()
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      "property_type": response.meta["property_type"]} 
            )
            
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        apartment_types = ["appartement", "apartment", "flat","penthouse", "duplex", "triplex", "maisonette",'residential complex']
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa',"terrace", "detach", "cottage"]
        studio_types = ["studio"]
        features_keys = ['furnish','unfurnish','semi-detach','detach','terrace','end-terrace','parking','lift','elevator','bedroom','bathroom','room']
        words_re = re.compile("|".join(apartment_types+house_types+studio_types+features_keys))
        availability = response.xpath('.//*[@class="availability"]/text()').extract_first()
        if 'let agreed' not in availability.lower():
            external_link= response.meta.get('request_url')
            item_loader.add_value('external_link',external_link)
            property_type = response.meta.get('property_type')
            item_loader.add_value('property_type',property_type)
            title = response.xpath('.//*[@name="viewport"]/following-sibling::title/text()').extract_first()
            if re.search(r'.*(?=\savailable)',title,re.IGNORECASE):
                title_new = remove_unicode_char(re.search(r'.*(?=\savailable)',title,re.IGNORECASE).group().lower().replace('currently',''))
                item_loader.add_value('title',remove_white_spaces(title_new.strip(' , ')))
            else:
                title=title.split("\u2013")[0]
                item_loader.add_value('title',title)

            title = item_loader.get_output_value('title')
            if title:
                address_str = title.strip(' , ')        
                if re.search(r'\w+\sin\s\w+',address_str):
                    address = ','.join(address_str.split('in')[1:])
                    item_loader.add_value('address',remove_white_spaces(address))
                else:
                    address = ','.join(address_str.split(',')[1:])
                    if not words_re.search(address):
                        item_loader.add_value('address',remove_white_spaces(address))
                    elif words_re.search(address):
                        address = ','.join(address_str.split(',')[:-1])
                        item_loader.add_value('address',remove_white_spaces(address))
            address = item_loader.get_output_value('address')
            zipcode1 = re.search(r'(GIR|[A-Za-z]\d[A-Za-z\d]?|[A-Za-z]{2}\d[A-Za-z\d]?)[ ]??(\d[A-Za-z]{0,2})??$',address,re.IGNORECASE)
            zipcode2 = re.search(r'(([A-Z][A-HJ-Y]?\d[A-Z\d]?|ASCN|STHL|TDCU|BBND|[BFS]IQQ|PCRN|TKCA) ?\d[A-Z]{2}|BFPO ?\d{1,4}|(KY\d|MSR|VG|AI)[ -]?\d{4}|[A-Z]{2} ?\d{2}|GE ?CX|GIR ?0A{2}|SAN ?TA1)$',address,re.IGNORECASE)
            
            if zipcode1:
                item_loader.add_value('zipcode',remove_white_spaces(zipcode1.group()))
            elif zipcode2:
                item_loader.add_value('zipcode',remove_white_spaces(zipcode2.group()))
            elif zipcode1==None and zipcode2 == None and len(address.split(','))>1:
                if ''.join(address.split(', ')[-1].split()).isalpha()==False:
                    item_loader.add_value('zipcode',remove_white_spaces(address.split(', ')[-1]))
    
            if zipcode1:
                city = remove_white_spaces(address.split(',')[-2])
                item_loader.add_value('city',city)
            elif zipcode2:
                city = remove_white_spaces(address.split(',')[-2])
                item_loader.add_value('city',city) 
            elif zipcode1==None and zipcode2 == None and len(address.split(','))>1:
                if ''.join(address.split(',')[-1].split()).isalpha()==False:
                    item_loader.add_value('city',remove_white_spaces(address.split(',')[-2]))
                else:
                    city = remove_white_spaces(address.split(',')[-1])
                    item_loader.add_value('city',city.strip(' , '))
            else:
                city = remove_white_spaces(address.split(',')[-1])
                item_loader.add_value('city',city.strip(' , '))
            date = response.xpath('.//*[@name="viewport"]/following-sibling::title/text()').extract_first()
            if date:
                date = re.search(r'(?<=available).{0,1}\d{1,2}/\d{1,2}/\d{2,4}',date.lower())
                if date:
                    availability_date = format_date(remove_white_spaces(date.group()), date_format="%d/%m/%Y")
                    item_loader.add_value('available_date',availability_date)
            room_count = response.xpath('.//*[@class="bedrooms"]/text()').extract_first()
            if room_count != '0' or room_count != None:
                item_loader.add_value('room_count',room_count)
            elif room_count == '0' or room_count == None and property_type == 'studio':
                item_loader.add_value('room_count','1')
            item_loader.add_xpath('bathroom_count','.//*[@class="bathrooms"]/text()')
            
            item_loader.add_xpath('external_id','.//*[@class="ref"]/text()')
            rent_string = response.xpath('.//*[@class="price"]/text()').extract_first()
            if any(word in rent_string.lower() for word in ['month','pm','pcm']):
                item_loader.add_value('rent_string',rent_string)
            elif any(word in rent_string.lower() for word in ['week','pw','pppw','pcw']):
                rent = extract_rent_currency(rent_string,InfinitipropertiesSpider)[0]*4
                item_loader.add_value('rent_string','£'+str(int(rent)))
            feat = response.xpath('.//*[@class="features"]//li/text()').extract()
            features = ', '.join(feat).lower()
            if 'unfurnished' in features and 'furnished' not in features:
                item_loader.add_value('furnished',False)
            elif title and 'unfurnished' in title.lower() and 'furnished' not in title.lower():
                item_loader.add_value('furnished',False)
            elif 'furnished' in features and 'unfurnished' not in features:
                item_loader.add_value('furnished',True)
            elif title and 'furnished' in title.lower() and 'unfurnished' not in title.lower():
                item_loader.add_value('furnished',True)
            if title and ('terrace' in features or 'terrace' in title.lower()):
                item_loader.add_value('terrace',True)
            if 'balcony' in features:
                item_loader.add_value('balcony',True)
            if 'swimming pool' in features or 'pool' in features:
                item_loader.add_value('swimming_pool',True)
            if 'parking' in features:
                item_loader.add_value('parking',True)
            if 'dishwasher' in features:
                item_loader.add_value('dishwasher',True)
            if 'lift' in features or 'elevator' in features:
                item_loader.add_value('elevator',True)
            item_loader.add_xpath('images','.//*[@class="propertyhive-main-image"]/@href')
            item_loader.add_xpath('description','.//*[@class="room"]//text()')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.split('_')[0].capitalize(), self.country, self.locale))
            item_loader.add_value('landlord_name','Infiniti Properties')
            item_loader.add_value('landlord_phone','0141 553 2677')
            item_loader.add_value('landlord_email','hello@infinitiproperties.com')
            self.position += 1
            item_loader.add_value('position', self.position)
            yield item_loader.load_item()
                    
