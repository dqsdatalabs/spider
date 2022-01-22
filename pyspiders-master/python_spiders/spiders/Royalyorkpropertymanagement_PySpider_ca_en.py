import math

import requests
import scrapy

import re
from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

counter = 2
pos =1
prob = ''


class Royalyorkpropertymanagement_PySpider_ca_en(scrapy.Spider):
    name = 'royalyorkpropertymanagement'
    allowed_domains = ['royalyorkpropertymanagement.ca']
    country = 'canada'
    locale = 'en'
    execution_type = 'testing'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    def start_requests(self):

        start_urls = ['https://royalyorkpropertymanagement.ca/properties/?status=for-rent'

                      ]
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        urls = response.xpath(f'.//div[@class="rh_page rh_page__map_properties"]//div[contains(@class,"rh_list_card__buttons")]//input[@class="rh_list_card_more_details"]//@onclick').extract()
        for x in range(len(urls)):
            url = urls[x].replace("location.href='", "").replace("'", "")
            yield Request(url=url, callback=self.parse_area)
        next_page = response.xpath(f'.//a[@class="rh_pagination__btn rh_pagination__next"]//@href').extract()
        if next_page:
            next_page = response.xpath(f'.//a[@class="rh_pagination__btn rh_pagination__next"]//@href').extract()[0]
            next_page = response.urljoin(next_page)
            yield Request(url=next_page, callback=self.parse)

    def parse_area(self, response):
        global pos
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        item_loader.add_value('external_source', self.external_source)
        title = str.strip(response.xpath(".//h1[@class='rh_page__title']//text()").extract()[0])
        item_loader.add_value('title', title)
        item_loader.add_value('property_type', "apartment")
        ren = str.strip(response.xpath(".//div[@class='rh_page__property_price']//p[@class='price']//text()").extract()[0])
        rent = int(ren[ren.find('$') + 1:ren.find('/')])
        item_loader.add_value('rent', int(rent*1.24))
        item_loader.add_value('currency', "CAD")
        item_loader.add_value('landlord_name', 'Royal York')
        item_loader.add_value('landlord_phone', '905-385-8150')
        item_loader.add_value('landlord_email', 'Toronto@RoyalYorkPM.com')
        description = "".join(response.css('.rh_content.shorten-text p::text').getall())
        desc = re.sub(r'\s+', ' ', description).strip()
        item_loader.add_value('description', desc)
        item_loader.add_value('position', pos)
        lst = response.xpath(".//span[@class='figure']//text()").extract()
        someText = response.css("script[type='application/ld+json']:contains('longitude')").getall()
        al=someText[0].split()
        ind=al.index('"longitude":')
        ind2=al.index('"latitude":')
        longitude= float(al[ind+1])
        if longitude > 0 :
            longitude=longitude*-1
        imgs = response.css('.swipebox img::attr(src)').getall()
        item_loader.add_value("images",imgs)
        item_loader.add_value("external_images_count", len(imgs))
        latitude=float(al[ind2+1].replace(',',""))
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']
        item_loader.add_value('latitude', str(latitude))
        item_loader.add_value('longitude', str(longitude))
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)



        if len(lst) >= 4:
            item_loader.add_value('room_count', int(lst[0].strip().replace("+Den", "").replace(" + Den", "")))
            item_loader.add_value('bathroom_count', math.floor(float(lst[1].strip())))
            item_loader.add_value('square_meters', int(int(lst[3].strip())/10.764))
            item_loader.add_value('parking', True)
        else:
            item_loader.add_value('room_count', int(lst[0].strip().replace("+Den", "").replace(" + Den", "")))
            item_loader.add_value('bathroom_count', math.floor(float(lst[1].strip())))
            item_loader.add_value('square_meters', int(int(int(int(lst[2].strip())/10.764))*10.764))
        util = response.xpath(".//div[@class='feature-name yes']//text()").extract()
        utils = response.xpath(".//div[@class='feature-name']//text()").extract()
        if "Diswahser" in util:
            item_loader.add_value('dishwasher', True)
        if 'Private Terrace' in util :
            item_loader.add_value('terrace', True)
        if 'Washer/Dryer' in utils :
            item_loader.add_value('washing_machine', True)
        if 'Balcony' in utils :
            item_loader.add_value('balcony', True)

        pos+=1
        yield item_loader.load_item()
