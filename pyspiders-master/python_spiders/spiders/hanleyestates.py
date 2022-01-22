# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import format_date, remove_white_spaces

class HanleyestatesSpider(scrapy.Spider):
    name = "hanleyestates"
    allowed_domains = ["hanleyestates.com"]
    start_urls = (
        'http://www.hanleyestates.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        for page in range(1, 10):
            start_urls = "https://hanleyestates.com/rent/page/{}/".format(page)
            yield scrapy.Request(
                url=start_urls,
                callback=self.parse, 
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[contains(@class, "property_listing")]/h4/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True)
    
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        title = response.xpath('//h1[contains(@class, "entry-title")]/text()').extract_first('')
        if 'house' in title.lower():
            property_type = 'house'
        else:
            property_type = 'apartment'
        external_link = response.url
        city = response.xpath('//strong[contains(text(), "City")] /following-sibling::a/text()').extract_first('').strip()
        street = response.xpath('//strong[contains(text(), "State")] /following-sibling::a/text()').extract_first('').strip()
        area = response.xpath('//strong[contains(text(), "Area")] /following-sibling::a/text()').extract_first('').strip()
        zipcode = response.xpath('//strong[contains(text(), "Postcode")]/following-sibling::text()').extract_first('').strip()
        address = street + ' ' + area + ' ' + city + ' ' + zipcode
        external_id = response.xpath('//strong[contains(text(), "Property Id")]/following-sibling::text()').extract_first('').strip()
        bathrooms = response.xpath('//strong[contains(text(), "Bathrooms")]/following-sibling::text()').extract_first('').strip()
        room_count_text = response.xpath('//strong[contains(text(), "Bedrooms")]/following-sibling::text()').extract_first('')
        if room_count_text:
            try:
                room_count = re.findall(r'\d+', room_count_text)[0]    
            except:
                room_count = ''
        else:
            room_count = ''
        furnished_text = response.xpath('//strong[contains(text(), "Furnished")]/following-sibling::text()').extract_first('').strip()
        if 'yes' in furnished_text.lower():
            furnished = True     
        else:
            furnished = ''
        floor_text = response.xpath('//strong[contains(text(), "Floor")]/following-sibling::text()').extract_first('').strip()
        try: 
            if int(floor) > 0:
                floor = floor_text
            else:
                floor = ''
        except:
            floor = ''
        avaiable_date_text = response.xpath('//strong[contains(text(), "Available")]/following-sibling::text()').extract_first('').strip()
        if avaiable_date_text: 
            avaiable_date = format_date(remove_white_spaces(avaiable_date_text),'%d-%B-%Y')
        else:
            avaiable_date = ''
        energy_label = response.xpath('//strong[contains(text(), "Energy")]/following-sibling::text()').extract_first('').strip()
        features = response.xpath('//div[@id="collapseThree"]//div[contains(@class, "listing_detail")]//text()').extract()
        dishwasher = ''
        wash_machine = ''
        lift = ''
        balcony = ''
        for feature in features:
            if 'dishwasher' in feature.lower():
                dishwasher = True
            if 'washing machine' in feature.lower():
                wash_machine = True
            if 'lift' in feature.lower():
                lift = True
            if 'balcony' in feature.lower():
                balcony = True 
        image_urls = []
        images = response.xpath('//div[contains(@class, "image_gallery")]')
        for img in images:
            img_url = img.xpath('./@style').extract_first()
            img_text = re.search(r'url\((.*?)\)', img_url).group(1) 
            image_urls.append(img_text)
        
        rent_week = response.xpath('//strong[contains(text(), "Price")]/following-sibling::span[contains(@class,"price_label")]//text()').get()
        if rent_week and "pw" in rent_week.lower():
            rent = response.xpath('//strong[contains(text(), "Price")]/following-sibling::text()').get()
            if rent:
                rent = rent.replace("£","").strip().replace(",","")
                rent = int(float(rent))*4
        else:
            rent = response.xpath('//strong[contains(text(), "Price")]/following-sibling::text()').get()
            if rent:
                rent = rent.replace("£","").strip().replace(",","")
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('title', title)
        item_loader.add_value('external_id', str(external_id))
        if zipcode:
            item_loader.add_value('zipcode', zipcode)
        elif not zipcode and title:
            item_loader.add_value('zipcode', title.strip().split(" ")[-1])

        item_loader.add_value('city', city)
        item_loader.add_value('address', address)
        item_loader.add_xpath('description', '//div[@class="wpestate_property_description"]/p//text()')
        item_loader.add_value('rent', rent)
        item_loader.add_value("currency", "GBP")
        if bathrooms:
            item_loader.add_value('bathroom_count', str(bathrooms))
        if dishwasher:
            item_loader.add_value('dishwasher', True)
        if wash_machine:
            item_loader.add_value('washing_machine', True) 
        if lift:
            item_loader.add_value('elevator', True) 
        if balcony: 
            item_loader.add_value('balcony', True)
        if floor:
            item_loader.add_value('floor', str(floor))
        if avaiable_date:
            item_loader.add_value('available_date', avaiable_date)
        if energy_label:
            item_loader.add_value('energy_label', energy_label)
        if furnished:
            item_loader.add_value('furnished', True)
        item_loader.add_value('images', image_urls)
        if room_count:
            item_loader.add_value('room_count', str(room_count))
        item_loader.add_xpath("latitude", "//div[@id='googleMap_shortcode']/@data-cur_lat[.!='0']")
        item_loader.add_xpath("longitude", "//div[@id='googleMap_shortcode']/@data-cur_long[.!='0']")
        item_loader.add_value('landlord_name', 'Hanley Estates')
        item_loader.add_value('landlord_email', 'info@hanleyestates.com')
        item_loader.add_value('landlord_phone', '0207 263 3388')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item() 