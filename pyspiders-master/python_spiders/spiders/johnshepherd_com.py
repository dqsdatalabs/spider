import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class JohnshepherdComSpider(scrapy.Spider):
    name = 'johnshepherd_com'
    allowed_domains = ['johnshepherd.com']
    start_urls = ['https://johnshepherd.com/lettings/property-to-rent-in-birmingham/?r_s=1634667260&area=&proptype=&furnish=&radius=&minprice=57.54&maxprice=579.37&minbeds=1&maxbeds=5&minprice=&maxprice=&minbeds=&maxbeds=&type=lettings&officeid=rps_loc-JAE%2Crps_loc-JSU%2Crps_loc-JSB%2Crps_loc-JSC%2Crps_loc-JSE%2Crps_loc-JSH%2Crps_loc-JSN%2Crps_loc-JSL%2Crps_loc-JSS%2Crps_loc-JSA%2Crps_loc-JSD%2Crps_loc-JSV%2Crps_loc-JSX&sortby=price&sortdescending=1']
    country = 'united_Kingdom'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        for appartment in response.css("#rit_properties_results > ul > li"):
            yield Request(appartment.css("a.rit_results_more ").attrib['href'],
                          callback=self.populate_item
                          )
        try:
            next_page = response.xpath(
                "//a[contains(.,'Next')]/@href").get()
        except:
            next_page = None

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            'body > div.l-canvas.sidebar_none.type_wide > div.l-main > div > main > section > div > div:nth-child(6) > div > div.rit_property_details_section_0 > div:nth-child(1)::text').get()
        zipcode = title.split(", ")[-1]
        city = title.split(", ")[1]

        rent = response.css('body > div.l-canvas.sidebar_none.type_wide > div.l-main > div > main > section > div > div:nth-child(6) > div > div.rit_property_details_section_0 > div:nth-child(2)::text').get().split("£")[
            1].split(' ')[0]
        if "," in rent:
            rent = rent.split(',')
            rent = rent[0] + rent[1]

        description = ""
        description_array = response.css(
            "table > tr > td:nth-child(1) > div:nth-child(2) > p:nth-child(1)::text").extract()

        for item in description_array:
            description += item

        available_date = description_array[-1].split("from: ")[-1]

        furnished = None
        if "furnished" in description:
            furnished = True
        if "unfurnished" in description:
            furnished = False

        images = response.css(
            '#myCarousel > div > div > img::attr(src)').extract()

        # floor_plans = response.css(
        #     'img.floorplans::attr(src)').extract()

        # features = response.css("ul.features>li::text").extract()

        rooms = response.css(
            'ul.icons-row > li:nth-child(1) > span::text').get()
        bathrooms = response.css(
            'ul.icons-row > li:nth-child(3) > span::text').get()
        # floor = response.css('span.propertytype::text').get()

        coords = response.css('#property_map > iframe::attr(src)').get().split(
            "@")[1].split('&')[0]
        lat = coords.split(',')[0]
        lng = coords.split(',')[1]

        # parking = None
        # furnished = None
        # for item in features:
        #     if "parking" in item:
        #         parking = True
        #     elif "furnished" in item:
        #         furnished = True

        # if "house" in response.meta['property_type']:
        #     property_type = 'house'
        # else:
        #     property_type = 'apartment'

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "external_id", "{}".format(id.split("=")[-1].strip()))
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        # # item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        # item_loader.add_value("floor", floor)
        item_loader.add_value("address", title)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        # item_loader.add_value("parking", parking)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("available_date", available_date)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        # item_loader.add_value("floor_plan_images", floor_plans)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "GBP")

        # LandLord Details
        item_loader.add_value("landlord_phone", "0121 647 5444")
        item_loader.add_value("landlord_email", "birmingham@johnshepherd.com")
        item_loader.add_value("landlord_name", "John Shepherd")

        yield item_loader.load_item()
