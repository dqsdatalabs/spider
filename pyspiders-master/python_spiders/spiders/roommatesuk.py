# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re, json
from ..loaders import ListingLoader
from ..helper import format_date, remove_white_spaces

def getSqureMtr(text):
    list_text = re.findall(r'\d+',text)

    if len(list_text) == 3:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 2:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 1:
        output = int(list_text[0])
    else:
        output=0

    return int(output)

class RoommatesukSpider(scrapy.Spider):
    name = "roommatesuk"
    allowed_domains = ["roommatesuk.com"]
    start_urls = (
        'http://www.roommatesuk.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://roommatesuk.com/rooms-for-rent-2+bedroom-show-advanced-rent-search', 'property_type': 'apartment'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//a[contains(text(), "View Details")]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_lat_lon, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_lat_lon(self, response):
        zipcode = re.search(r'var zip = \"(.*?)\"', response.text).group(1)
        district = re.search(r'var district = \"(.*?)\"', response.text).group(1) 
        address = re.search(r'var address = \"(.*?)\"', response.text).group(1)  
        url = f"https://maps.googleapis.com/maps/api/geocode/json?key=AIzaSyCH7P4BACIE9l0207xW7HF4dnuChm4rbaI&address={district},{address},{zipcode},uk"
        yield scrapy.Request(url=url, callback=self.get_latlon_value, dont_filter=True, meta={'property_type': response.meta.get('property_type'), 'url': response.url})
    
    def get_latlon_value(self, response):
        data = json.loads(response.text)
        lat = data['results'][0]['geometry']['location']['lat']
        lon = data['results'][0]['geometry']['location']['lng']
        yield scrapy.Request(url=response.meta.get('url'), callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type'), 'lat': lat, 'lon': lon })
    
    def get_property_details(self, response):
        # parse details of the propery
        property_type = response.meta.get('property_type')
        external_link = response.url
        address = response.xpath('//label[contains(text(), "Address")]/following-sibling::span/text()').extract_first()
        rent_string = response.xpath('//label[contains(text(), "Rent")]/following-sibling::span/text()').extract_first('').strip()
        rent_value = re.findall(r'([\d|,|\.]+)', rent_string)[0].replace(',', '')
        rent_month = str(int(rent_value) * 4) + '£'
        try:
            zipcode = re.search(r'var zip = \"(.*?)\"', response.text).group(1)
        except:
            zipcode = ''
        blecony = response.xpath('//label[contains(text(), "Balcony")]/following-sibling::span/text()').extract_first()
        if 'yes' in blecony.lower():
            blecony = True
        else:
            blecony = ''
        details = response.xpath('//div[@id="surrounding-list"]/ul/li')
        swimming_pool = ''
        for detail in details:
            text = detail.xpath('./text()').extract_first().strip()
            if 'swimming pool' in text.lower():
                swimming_pool = True        

        available_date_text = response.xpath('//label[contains(text(), "Available")]/following-sibling::span/text()').extract_first('')
        if available_date_text:
            available_date = format_date(remove_white_spaces(available_date_text), '%d %B %Y')
        else:
            available_date = ''
        city_text = response.xpath('//label[contains(text(), "Area")]/following-sibling::span/text()').extract_first('').strip()
        deposit_text = response.xpath('//label[contains(text(), "Deposit")]/following-sibling::span/text()').extract_first('').strip()     
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_link.split("-")[-1])
        item_loader.add_xpath('description', '//div[@class="desc"]/p/text()')
        item_loader.add_value('address', address)
        if zipcode:
            item_loader.add_value('zipcode', zipcode) 
        if blecony:
            item_loader.add_value('balcony', True)    
        if city_text:
            item_loader.add_value('city', city_text.split(' ')[-1])
        if swimming_pool:
            item_loader.add_value('swimming_pool', True) 
        if deposit_text:
            item_loader.add_value('deposit', getSqureMtr(deposit_text))
        if available_date:
            item_loader.add_value('available_date', available_date) 
        parking = response.xpath('//li[contains(text(), "Garage")]/text()').extract_first()
        if parking:
            item_loader.add_value('parking', True)            
        item_loader.add_xpath('title', '//title/text()')
        item_loader.add_value('latitude', str(response.meta.get('lat')))
        item_loader.add_value('longitude', str(response.meta.get('lon')))
        item_loader.add_value('rent_string', rent_month)
        item_loader.add_xpath('images', '//div[@class="item"]/img/@src')
        item_loader.add_value('room_count', '2')
        item_loader.add_value('landlord_name', 'Room Mates UK')
        item_loader.add_value('landlord_email', 'roommatesuk@gmail.com')
        item_loader.add_value('landlord_phone', '+36 1 400 90 21')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item() 