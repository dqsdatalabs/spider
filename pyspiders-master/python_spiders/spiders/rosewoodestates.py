# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
import dateparser
from ..loaders import ListingLoader

class RosewoodestatesSpider(scrapy.Spider):
    name = "rosewoodestates"
    allowed_domains = ["rosewoodestates.co.uk"]
    start_urls = (
        'http://www.rosewoodestates.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        for page in range(0, 8):
            start_urls = 'https://rosewoodestates.co.uk/advanced-search-2/page/{}/?filter_search_action%5B0%5D=lettings&adv6_search_tab=lettings&term_id=905&term_counter=0&location&no-of-beds&min-price&max-price&submit=Search%20Properties'.format(page)
            yield scrapy.Request(url=start_urls, callback=self.parse, dont_filter=True)
    def parse(self, response, **kwargs):
        links = response.xpath('//div[contains(@class, "property_listing")]//h4/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True)
    def get_property_details(self, response):
        title = response.xpath('//h1[contains(@class, "entry-title")]/text()').extract_first()
        if 'house' in title.lower():
            property_type = 'house'
        elif 'apartment' in title.lower():
            property_type = 'apartment'
        elif 'flat' in title.lower():
            property_type = 'apartment' 
        else:
            property_type = ''
        address = title.replace(title.split(', ')[0], '').replace(',', ' ')
        if property_type and address:
            external_link = response.url
            external_id = response.xpath('//div[@id="propertyid_display"]/text()').extract_first()
            room_count_text = response.xpath('//li[contains(text(), "Bedroom")]/text()').extract_first('').strip()
            if room_count_text:
                room_count = re.findall(r'\d+', room_count_text)[0]     
            bathrooms_text = response.xpath('//li[contains(text(), "Bathroom")]/text()').extract_first('').strip() 
            if bathrooms_text:
                bathrooms = re.findall(r'\d+', bathrooms_text)[0]
            rent_string = ''.join(response.xpath('//div[@class="price_area"]/text()').extract())
            iamges = response.xpath('//div[@class="gallery_wrapper"]/div[contains(@class, "image_gallery")]')
            img_value = []
            for img in iamges:
                img_url = img.xpath('./@style').extract_first()
                image = re.search(r'url\((.*?)\)', img_url).group(1)
                img_value.append(image)
            lat = response.xpath('//div[@id="googleMap_shortcode"]/@data-cur_lat').extract_first('').strip()
            lon = response.xpath('//div[@id="googleMap_shortcode"]/@data-cur_long').extract_first('').strip()
            zipcode = response.xpath('//strong[contains(text(), "Zip")]/following-sibling::text()').extract_first('').strip()
            city = address.split(', ')[-1]
            detail_texts = ''.join(response.xpath('//div[@id="wpestate_property_description_section"]//p/text()').extract())
            dates = response.xpath('//li[@class="first_overview_date"]/text()').extract_first('').strip()
            try:
                date_parsed = dateparser.parse( dates, date_formats=["%d %B %Y"] ) 
                date2 = date_parsed.strftime("%Y-%m-%d")
            except:
                try:
                    date2 =  format_date( dates,  '%d %B %Y')
                except:
                    date2 = ''
            energy_label = response.xpath('//strong[contains(text(), "Energy index")]/following-sibling::text()').extract_first('').strip()
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('external_id', str(external_id))
            item_loader.add_value('address', address)
            item_loader.add_xpath('description', '//div[@id="wpestate_property_description_section"]/p//text()')
            item_loader.add_value('rent_string', rent_string)
            item_loader.add_xpath('title', '//h1[contains(@class, "entry-title")]/text()')
            if zipcode:
                item_loader.add_value('zipcode', zipcode) 
            item_loader.add_value('images', img_value)
            if lat: 
                item_loader.add_value('latitude', str(lat))
            if lon: 
                item_loader.add_value('longitude', str(lon))
            if date2:
                item_loader.add_value('available_date', date2)
            if energy_label:
                item_loader.add_value('energy_label', energy_label)
            item_loader.add_value('city', city)
            item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('bathroom_count', str(bathrooms))
            item_loader.add_value('landlord_name', 'Rosewood Estates')
            item_loader.add_value('landlord_email', 'info@rosewoodestates.co.uk')
            item_loader.add_value('landlord_phone', '0203 759 8950')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item()