# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import json
import scrapy
import re
import math
from geopy import Nominatim
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
from ..helper import extract_number_only
from ..loaders import ListingLoader
from ..user_agents import random_user_agent


class JohndwoodSpider(scrapy.Spider):
    name = 'johndwood_co_uk'
    allowed_domains = ['johndwood.co.uk']
    start_urls = ['https://www.johndwood.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            {
                'property_type': 'house',
                'url': 'https://www.johndwood.co.uk/rent/search/page-1/pricing-monthly/house/'},
            {
                'property_type': 'apartment',
                'url': 'https://www.johndwood.co.uk/rent/search/page-1/pricing-monthly/flat/'},
            {
                'property_type': 'house',
                'url': 'https://www.johndwood.co.uk/rent/search/page-1/pricing-monthly/bungalow/'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse,
                meta={'property_type': url.get('property_type'),
                      'request_url': url.get('url')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[contains(@href, "rent/property/")]/@href').extract()
        for property_url in listings:
            yield scrapy.Request(url=response.urljoin(property_url),
                                 callback=self.get_property_details,
                                 meta={'request_url': response.urljoin(property_url),
                                       'property_type': response.meta['property_type']})

        if len(listings) > 0:
            current_page = re.findall(r"(?<=/page-)\d+", response.meta['request_url'])[0]
            next_page_url = re.sub(r"(?<=/page-)\d+", str(int(current_page) + 1), response.meta['request_url'])
            yield scrapy.Request(url=next_page_url,
                                 callback=self.parse,
                                 meta={'request_url': next_page_url,
                                       'property_type': response.meta['property_type']})

    def get_property_details(self, response):
        property_script = response.xpath('.//script[@id="ng-details-panel"]/text()').extract_first()
        property_data = HtmlResponse(url="property_script", body=property_script, encoding="utf-8")

        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', re.findall(r"(?<=ref-)\d+", response.meta['request_url'])[0])
        title = property_data.xpath('.//span[@class="details-panel__title-sub"]/text()').extract_first()
        if title:
            item_loader.add_value('title', title)

        rent = property_data.xpath('.//p[@class="details-panel__details-text-primary"]/text()').extract_first()
        if rent:
            item_loader.add_value('rent_string', re.findall(r"[^a-z]*pcm", rent)[0])

        item_loader.add_xpath('description', './/div[@class="details-frame__primary"]//p/text()')
        coordinates = response.xpath('.//div[@data-details-type="property"]/@data-location').extract_first()
        if coordinates:
            latitude = re.findall(r"(?<=lat:)[^,]*", coordinates)[0].strip()
            longitude = re.findall(r"(?<=lng:)\s*[^\s]*", coordinates)[0].strip()
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)
        item_loader.add_xpath('address', './/ul[@itemprop="address"]/li/text()')
        item_loader.add_xpath('zipcode', './/li[@itemprop="postalCode"]/text()')
        city = response.xpath('//li[@itemprop="addressRegion"]/text()').get()
        if city:
            item_loader.add_value('city', city)
        else:
            city = response.xpath('//li[@itemprop="addressLocality"]/text()').get()
            if city:
                item_loader.add_value('city', city)

        img_element = response.xpath('.//script[@id="ng-primary-panel"]/text()').extract_first()
        img_data = HtmlResponse(url="url", body=img_element, encoding="utf-8")
        images = img_data.xpath('.//div[@class="carousel__image"]/img/@src').extract()
        if len(images) > 0:
            item_loader.add_value('images', images)

        bed_bath_string = property_data.xpath('.//span[@class="details-panel__spec-list-number"]/text()').extract()
        if bed_bath_string:
            item_loader.add_value('room_count', bed_bath_string[0])
            item_loader.add_value('bathroom_count', bed_bath_string[1])

        square_meters = response.xpath('.//div[@class="details-frame__primary"]//p[contains(text(), "Sq")]/text()').extract_first()
        if square_meters and len(re.findall(r"[^a-z]*sq[^\w]*m", square_meters.lower())) > 0:
            square_meters = re.findall(r"[^a-z]*sq[^\w]*m", square_meters.lower())[0]
            # square_meters = re.sub(r"[^\d\.]+", "", square_meters)
            square_meters = re.sub(r"^[^\d]*", "", re.sub(r"[^\d]*$", "", square_meters))
            item_loader.add_value('square_meters', str(math.ceil(float(square_meters))))
        elif square_meters and len(re.findall(r"[^a-z]*sq[^\w]*ft", square_meters.lower())) > 0:
            square_meters = re.findall(r"[^a-z]*sq[^\w]*ft", square_meters.lower())[0]
            if len(re.findall(r"[\d\.,]+", square_meters)) > 0:
                square_meters = re.findall(r"[\d\.,]+", square_meters)[0]
                item_loader.add_value('square_meters', str(math.ceil(float(extract_number_only(square_meters)) * 0.092903)))

        # energy_label = response.xpath('.//p[contains(text(), "EPC Rating")]/text()').extract_first()
        energy_label = response.xpath('(.//p[contains(text(), "EPC Rating")]/text())[last()]').extract_first()
        if energy_label and len(re.findall(r"(?<=epc rating)[^\w]*[a-z][^\w]", energy_label.lower() + " ")) > 0:
            energy_label = re.findall(r"(?<=epc rating)[^\w]*[a-z][^\w]", energy_label.lower() + " ")[0].strip()
            item_loader.add_value('energy_label', re.sub(r"[^\w]*", "", energy_label))
        if response.xpath('//h4[@class="details-frame__content-title"]/text()').get():
            item_loader.add_xpath('landlord_name', './/h4[@class="details-frame__content-title"]/text()')
            item_loader.add_xpath('landlord_phone', './/span[contains(@class,"text--phone")]/text()')
        else:
            item_loader.add_value("landlord_name", "John D Wood & Co.")
            item_loader.add_value("landlord_phone", "020 3151 0998")
        item_loader.add_value("landlord_email", "hq@johndwood.co.uk")

        floor_element = response.xpath('.//script[@id="ng-floorplan-slide"]/text()').extract_first()
        floor_data = HtmlResponse(url="url", body=floor_element, encoding="utf-8")
        floor_img = floor_data.xpath('.//img[@class="slide__item-image"]/@src').extract()
        floor_img = [f"https://www.johndwood.co.uk{i}" for i in floor_img]
        item_loader.add_value('floor_plan_images', floor_img)

        
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Johndwood_PySpider_{}_{}".format(self.country, self.locale))

        status = response.xpath("//script[@id='ng-details-panel']//text()").get()
        if status:
            sel = Selector(text=status, type="html")
            let_agreed = sel.xpath("//span[contains(@class,'tag__text')]//text()[contains(.,'LET AGREED')]")
            if let_agreed:
                return
            else:
                yield item_loader.load_item()
        else:
            yield item_loader.load_item()