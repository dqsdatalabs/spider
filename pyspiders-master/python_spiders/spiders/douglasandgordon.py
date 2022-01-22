# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import js2xml
import re
from ..loaders import ListingLoader
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date
from datetime import date
# from geopy.geocoders import Nominatim
# from ..user_agents import random_user_agent


class DouglasandgordonSpider(scrapy.Spider):

    name = 'DouglasandGordon_PySpider_ireland_en'
    allowed_domains = ['douglasandgordon.com']
    start_urls = ['https://www.douglasandgordon.com/rent/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    # def __init__(self):
    #     self.position = 0

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.douglasandgordon.com/rent/list/anywhere/houses/?usersearch=true',
                'property_type': 'house'},
            {'url': 'https://www.douglasandgordon.com/rent/list/anywhere/flats/?usersearch=true',
                'property_type': 'house'},
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[@class="cta-link"]/@href').getall()
        for property_item in listings:
            yield scrapy.Request(
                url=f"https://www.douglasandgordon.com{property_item}",
                callback=self.get_property_details,
                meta={'request_url': f"https://www.douglasandgordon.com{property_item}",
                      'property_type': response.meta.get('property_type')}
            )


        next_page_url = response.xpath('.//a[@aria-label="Next"]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta.get('property_type')}
                )

    def get_property_details(self, response):

        external_link = response.meta.get('request_url')
        property_type = response.meta.get('property_type')
        external_id = re.findall(r"/\d+/", external_link)[0].replace("/", "")
        # geolocator = Nominatim(user_agent=random_user_agent())
        features = ' '.join(response.xpath('.//dt[contains(text(),"Additional features")]/following::dd//text()').extract())

        landlord_name = response.xpath('//h5[@class="contact-name"]/text()').extract_first()
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Douglasandgordon_PySpider_united_kingdom_en")

        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_id)
        item_loader.add_xpath('title', '//h1[contains(@class, "property-name")]/text()')
        item_loader.add_xpath('address', '//h1//text()')
        rent = response.xpath("//span[@class='h2 m-0']/text()").get()
        if rent:
            rent = "".join(filter(str.isnumeric, rent.split('.')[0].replace(',', '').replace('\xa0', '')))
            if rent.isdigit():
                item_loader.add_value("rent", str(int(float(rent)*4)))
        item_loader.add_value("currency", "GBP")

        city = response.xpath("//div[contains(@class,'property-brief')]/div/h1/text()").extract_first()
        if city:
            item_loader.add_value('city', city.split(",")[0].strip())
            item_loader.add_value('zipcode', city.split(",")[-1].strip())

        item_loader.add_xpath('description', './/div[@class="property-details-description"]/text()')
        item_loader.add_xpath('square_meters', './/span[contains(@class,"property-space")]/br/following-sibling::text()')
        # item_loader.add_xpath('images', './/div[@id="property-full-gallery"]//img/@href')
        item_loader.add_xpath('images', './/div[@id="propertyPicsSlider"]//img[contains(@class, "property-img")]/@*[name()="src" or name()="data-lazy"]')
        item_loader.add_xpath('bathroom_count', './/li[contains(@class, "ico-bathroom")]/text()')
        # unable to extract energy from image
        # item_loader.add_xpath('energy_label', '')
        item_loader.add_xpath('room_count', './/li[contains(@class, "ico-bedroom")]/text()')

        item_loader.add_xpath('floor_plan_images', './/div[@class="floorplan"]//img/@src')

        longitude = response.xpath('.//a[@class="js-alert-map"]/@data-lng').extract_first()
        latitude = response.xpath('.//a[@class="js-alert-map"]/@data-lat').extract_first()
        item_loader.add_value('latitude', latitude)
        item_loader.add_value('longitude', longitude)
        # location = geolocator.reverse(f"{latitude},{longitude}")
        # if location:
        #     item_loader.add_value('address', location.address)
        #     if "postcode" in location.raw['address']:
        #         item_loader.add_value('zipcode', location.raw['address']['postcode'])
        #     if "city" in location.raw['address']:
        #         item_loader.add_value('city', location.raw['address']['city'])
        # ex https://www.douglasandgordon.com/rent/property-details/64350/alexander-square-sw3/
        if "parking" in features:
            item_loader.add_value('parking', True)
        # set because of
        # ex https://www.douglasandgordon.com/rent/property-details/47356/winchendon-road-sw6/
        if "balcony" in features:
            item_loader.add_value('balcony', True)
        # ex https://www.douglasandgordon.com/rent/property-details/44452/princes-gate-sw7/
        if "terrace" in features:
            item_loader.add_value('terrace', True)
        # self.position += 1
        # item_loader.add_value('position', self.position)

        name = response.xpath("//div[contains(@class,'contact-details')]/h5[@class='contact-name']/text()").extract_first()
        if name:
            item_loader.add_value("landlord_name", name.split(" ")[1].strip())

        
        item_loader.add_xpath("landlord_email", "//div[contains(@class,'contact-details')]/a/text()")
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'contact-details')]/h4[@class='tel']//text()")


        yield item_loader.load_item()
