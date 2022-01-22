# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import  extract_rent_currency, remove_white_spaces
import re
import js2xml
import lxml.etree
from parsel import Selector

class TaylorgibbsSpider(scrapy.Spider):
    
    name = 'taylorgibbs_co_uk'
    allowed_domains = ['taylorgibbs.co.uk']
    start_urls = ['https://www.taylorgibbs.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator=','
    scale_separator='.'
    position = 0
        
    def start_requests(self):
        start_urls = ['https://www.taylorgibbs.co.uk/search/1.html?instruction_type=Letting&minpricew=&maxpricew=&property_type=']
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url':url})
            
    def parse(self, response, **kwargs):
        for property_url in response.xpath('.//*[contains(text(),"MORE DETAILS")]/@href').extract():
            yield scrapy.Request(
                url='https://www.taylorgibbs.co.uk'+property_url,
                callback=self.get_property_details,
                meta={'request_url': 'https://www.taylorgibbs.co.uk'+property_url})
        
        if len(response.xpath('.//a[contains(text(),"MORE DETAILS")]')) > 0:
            current_page = re.findall(r"(?<=search/)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=search/)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url})
        
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.meta.get('request_url')
        external_id = re.search(r'(?<=property-details/)\d+',external_link).group()
        apartment_types = ["appartement", "apartment", "flat","penthouse", "duplex", "triplex", "maisonette"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa',"terrace", "detach", "cottage"]
        studio_types = ["studio"]
        rooms = ["room"]
        title = response.xpath('.//meta[@name="viewport"]/preceding-sibling::title/text()').extract_first()
        email = response.xpath('.//*[@class="mobile-btns-new"]/../a[contains(@href,"mailto:")]/@href').extract_first()
        tel = response.xpath('.//*[@class="mobile-btns-new"]/../a[contains(@href,"tel:")]/@href').extract_first()
        add_rent_str = response.xpath('.//*[@class="fees-head"]/preceding-sibling::text()').extract_first()
        room_count = response.xpath('.//*[contains(@alt,"bedroom")]/following-sibling::strong[1]/text()').extract_first()
        bathroom_count = response.xpath('.//*[contains(@alt,"bathroom")]/following-sibling::strong[1]/text()').extract_first()
        feat_raw = response.xpath('.//*[contains(text(),"Property Description")]/following-sibling::ul//li/text()').extract()
        feat_list = list(filter(lambda x:x.strip(),feat_raw))
        features = ' '.join(feat_list).lower()
        period = response.xpath('.//*[@class="fees-head"]').extract_first()
        # rent_string = remove_white_spaces(add_rent_str.split('-')[-1])
        address = remove_white_spaces(add_rent_str.split('-')[0])
        rent = "".join(response.xpath("//span[contains(@class,'blue-text')]/text()").getall())
        if rent:
            rent = rent.split("£")[1].strip()
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "GBP")

        item_loader.add_value('address',address)
        zipcode1 = re.search(r'(GIR|[A-Za-z]\d[A-Za-z\d]?|[A-Za-z]{2}\d[A-Za-z\d]?)[ ]??(\d[A-Za-z]{0,2})??$',address,re.IGNORECASE)
        zipcode2 = re.search(r'(([A-Z][A-HJ-Y]?\d[A-Z\d]?|ASCN|STHL|TDCU|BBND|[BFS]IQQ|PCRN|TKCA) ?\d[A-Z]{2}|BFPO ?\d{1,4}|(KY\d|MSR|VG|AI)[ -]?\d{4}|[A-Z]{2} ?\d{2}|GE ?CX|GIR ?0A{2}|SAN ?TA1)$',address,re.IGNORECASE)
            
        if zipcode1:
            item_loader.add_value('zipcode',remove_white_spaces(zipcode1.group()))
            city = remove_white_spaces(address.rstrip(zipcode1.group())).rstrip(',')
            city = remove_white_spaces(city.split(',')[-1])
            item_loader.add_value('city',city)
        elif zipcode2:
            item_loader.add_value('zipcode',remove_white_spaces(zipcode2.group()))
            city = remove_white_spaces(address.rstrip(zipcode2.group())).rstrip(',')
            city=remove_white_spaces(city.split(',')[-1])
            item_loader.add_value('city',city) 
        elif zipcode1==None and zipcode2==None and len(address.split(', '))>1:
            if ''.join(remove_white_spaces(address.split(',')[-1]).split()).isalpha()==False:
                city_zip = remove_white_spaces(address.split(',')[-1])
                city_zip = re.sub(('-|–|—'),' ',city_zip)
                if re.findall(r'\s',remove_white_spaces(city_zip)).count(' ')==0:
                    item_loader.add_value('zipcode',city_zip)
                    item_loader.add_value('city',remove_white_spaces(address.split(',')[-2]))
                elif re.findall(r'\s',remove_white_spaces(city_zip)).count(' ')>0:
                    zipp = remove_white_spaces(city_zip).split(' ')[:-1]
                    if ''.join(zipp).isalpha()==True:
                        item_loader.add_value('city',' '.join(zipp))
                        item_loader.add_value('zipcode',city_zip.split(' ')[-1])
                    elif ''.join(zipp).isalpha()==False:
                        item_loader.add_value('zipcode',' '.join(city_zip.split(' ')[-2:]))
                        zipps = item_loader.get_output_value('zipcode')
                        item_loader.add_value('city',remove_white_spaces(city_zip.rstrip(zipps)))
            else:
                city = remove_white_spaces(address.split(',')[-1])
                item_loader.add_value('city',city.strip(','))
        else:
            city = remove_white_spaces(address.split(',')[-1])
            item_loader.add_value('city',city.strip(','))
            
        item_loader.add_xpath('description','.//*[contains(text(),"Property Description")]/following-sibling::p/text()')
        description = item_loader.get_output_value('description')
        
        if any (i in remove_white_spaces(features).lower() for i in studio_types) or any (i in remove_white_spaces(description).lower() for i in studio_types):
            item_loader.add_value('property_type', "studio")
        elif any (i in remove_white_spaces(features).lower() for i in apartment_types) or any (i in remove_white_spaces(description).lower() for i in apartment_types):
            item_loader.add_value('property_type', "apartment")
        elif any (i in remove_white_spaces(features).lower() for i in house_types) or any (i in remove_white_spaces(description).lower() for i in house_types):
            item_loader.add_value('property_type', "house")
        elif any (i in remove_white_spaces(features).lower() for i in rooms) or any (i in remove_white_spaces(description).lower() for i in rooms):
            item_loader.add_value('property_type', "room")
        
        property_type = item_loader.get_output_value('property_type')
        
        if room_count and remove_white_spaces(room_count) !='0':
            item_loader.add_value('room_count',remove_white_spaces(room_count))
        elif room_count and remove_white_spaces(room_count) =='0' and property_type=='studio':
            item_loader.add_value('room_count','1')
        elif room_count==None and property_type=='studio':
            item_loader.add_value('room_count','1')
        
        if bathroom_count and remove_white_spaces(bathroom_count)!='0':
            item_loader.add_value('bathroom_count',remove_white_spaces(bathroom_count))
        
        images = response.xpath('.//img[@itemprop="image"]/@src').extract()
        if images:
            images = ['https://www.taylorgibbs.co.uk'+image for image in images]
            item_loader.add_value('images',images)

        floor_plan_images = response.xpath('.//img[@class="floorplan img-responsive"]/@src').extract()
        if floor_plan_images:
            floor_plan_images = ['https://www.taylorgibbs.co.uk'+floor_plan_image for floor_plan_image in floor_plan_images]
            item_loader.add_value('floor_plan_images',floor_plan_images)

        javascript = response.xpath('.//script[contains(text(),"googlemap")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            lat_lng = selector.xpath('.//identifier[@name="opt"]/../number/@value').extract()
            if lat_lng:
                item_loader.add_value('latitude', lat_lng[0])
                item_loader.add_value('longitude', lat_lng[1])

        if re.search(r'un[^\w]*furnish',features):
            item_loader.add_value('furnished',False)
        elif re.search(r'not[^\w]*furnish',features):
            item_loader.add_value('furnished',False)
        elif re.search(r'furnish',features):
            item_loader.add_value('furnished',True)
        if re.search(r'dish[^\w]*washer',features):
            item_loader.add_value('dishwasher',True)
        if re.search(r'swimming[^\w]*pool',features) or 'pool' in features:
            item_loader.add_value('swimming_pool',True)
        if 'terrace' in features:
            item_loader.add_value('terrace',True)
        if 'balcony' in features:
            item_loader.add_value('balcony',True)
        if 'parking' in features:
            item_loader.add_value('parking',True)
        if 'lift' in features or 'elevator' in features:
            item_loader.add_value('elevator',True)
        if re.search(r'no[^\w]*pet\w{0,1}[^\w]*allow',features):
            item_loader.add_value('pets_allowed',False)
        elif re.search(r'pet\w{0,1}[^\w]*not[^\w]*allow',features):
            item_loader.add_value('pets_allowed',False)
        elif re.search(r'pet\w{0,1}[^\w]*allow',features):
            item_loader.add_value('pets_allowed',True)

        floor = "".join(response.xpath("//li[contains(.,'Floor')]/text()[not(contains(.,'Floorplan') or contains(.,'Floors'))]").getall())
        if floor:
            floor = floor.split("Floor")[0].strip()
            item_loader.add_value("floor", floor)
                
        item_loader.add_value('title',remove_white_spaces(title.split('|')[0]))
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id',external_id)
        item_loader.add_value("external_source", "Taylorgibbs_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('landlord_phone',tel.split(':')[-1])
        item_loader.add_value('landlord_email',email.split(':')[-1])
        item_loader.add_value('landlord_name','Taylor Gibbs')
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
